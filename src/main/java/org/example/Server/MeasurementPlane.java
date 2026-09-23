package org.example.Server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

import org.example.raft.ReadLevel;

/**
 * One node's measurement plane (Chameleon stage 3): the service-time
 * histograms and the occupancy meter, their refresh/control schedules, and
 * the CSV outputs. Utilization is computed per control interval but not yet
 * acted on; the price controller (stage 4) will read it.
 *
 * Levels are indexed 0..4 for the read levels (proto ordinal) and
 * 5..5+majority-1 for write concerns 1..majority.
 */
public final class MeasurementPlane implements AutoCloseable {

    // From config (see ExperimentConfig.chameleon). S_MAX is a placeholder
    // budget until the stage 6 load sweep calibrates it; utilization may
    // exceed 1 by design so the controller gets signal.
    private static volatile double S_MAX = 1000;
    private static volatile int CONTROL_INTERVAL_MS = 100;
    private static volatile double U_TARGET = 0.85;
    private static volatile double ETA = 1.0;
    private static volatile double LAMBDA_MIN = 0.0001;
    private static volatile double LAMBDA_MAX = 1.0e6;
    // From config (server.histogramDecay): service-time histogram decay per
    // refresh tick; effective memory ~ HISTOGRAM_REFRESH_MS / (1 - decay).
    private static volatile double HISTOGRAM_DECAY = 0.95;

    private static final int HISTOGRAM_REFRESH_MS = 100;
    private static final int HISTOGRAM_DUMP_INTERVAL_MS = 5000;

    // Step 4 cold-start rule: cap the requests concurrently riding a cell
    // that has no samples yet, since samples arrive only on completion.
    private static final int UNCALIBRATED_RIDER_CAP = 64;

    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        applyEconomics(config.server.sMax, config.chameleon.controlIntervalMs,
                config.chameleon.uTarget, config.chameleon.eta, config.chameleon.lambdaMin,
                config.chameleon.lambdaMax);
        HISTOGRAM_DECAY = config.server.histogramDecay;
    }

    /** Direct knob access for tests (overload shedding needs artificial pressure). */
    public static void applyEconomics(double sMax, int controlIntervalMs, double uTarget, double eta,
            double lambdaMin, double lambdaMax) {
        S_MAX = sMax;
        CONTROL_INTERVAL_MS = controlIntervalMs;
        U_TARGET = uTarget;
        ETA = eta;
        LAMBDA_MIN = lambdaMin;
        LAMBDA_MAX = lambdaMax;
    }

    public static double sMax() {
        return S_MAX;
    }

    private final int nodeId;
    private final int majority;
    private final ServiceTimeHistograms histograms;
    private final OccupancyMeter occupancy = new OccupancyMeter();
    private final ServerDrainMetrics drainMetrics;

    // Cross-check: completed callback service excludes pre-callback queueing,
    // while the authoritative occupancy integral includes it. The two agree
    // when callback queueing is negligible, apart from interval boundaries.
    private final DoubleAdder completedServiceMsInterval = new DoubleAdder();

    private final AtomicLong cumulativeSlotNanos = new AtomicLong();
    private final DoubleAdder cumulativeCompletedServiceMs = new DoubleAdder();

    private volatile double lastUtilization;
    private volatile double lastCrossCheckUtilization;

    private final PriceController priceController = new PriceController(U_TARGET, ETA, LAMBDA_MIN, LAMBDA_MAX);
    private final java.util.concurrent.atomic.AtomicInteger[][] uncalibratedRiders;

    private final ScheduledExecutorService scheduler;

    public MeasurementPlane(int nodeId, int numServers) {
        this.nodeId = nodeId;
        this.majority = (numServers / 2) + 1;
        this.histograms = new ServiceTimeHistograms(5 + majority, HISTOGRAM_DECAY);
        this.drainMetrics = new ServerDrainMetrics(nodeId);
        this.uncalibratedRiders = new java.util.concurrent.atomic.AtomicInteger[5 + majority][ServiceTimeHistograms.GAP_BUCKETS];
        for (int l = 0; l < 5 + majority; l++) {
            for (int g = 0; g < ServiceTimeHistograms.GAP_BUCKETS; g++) {
                uncalibratedRiders[l][g] = new java.util.concurrent.atomic.AtomicInteger();
            }
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "measurement-plane-" + nodeId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(histograms::refreshTick,
                HISTOGRAM_REFRESH_MS, HISTOGRAM_REFRESH_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::closeControlInterval,
                CONTROL_INTERVAL_MS, CONTROL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::dumpHistograms,
                HISTOGRAM_DUMP_INTERVAL_MS, HISTOGRAM_DUMP_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(drainMetrics::flush, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    // ===== Level indexing =====

    public int readLevelIndex(ReadLevel level) {
        return level.getNumber();
    }

    public int writeLevelIndex(int writeConcern) {
        return 5 + Math.min(Math.max(1, writeConcern), majority) - 1;
    }

    public String levelLabel(int levelIndex) {
        return levelIndex < 5 ? "R:" + ReadLevel.forNumber(levelIndex).name() : "W:" + (levelIndex - 4);
    }

    // ===== Hot path =====

    /** An ingress-admitted frame holds a slot from here through its terminal reply. */
    public void requestAdmitted() {
        occupancy.onEvent(1);
    }

    ServerDrainMetrics drainMetrics() {
        return drainMetrics;
    }

    /** Close slots cancelled after transport admission and before completion. */
    public void requestsAbandonedBeforeCallback(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("abandoned request count must be positive, got " + count);
        }
        if (count > occupancy.inFlight()) {
            throw new IllegalStateException("abandoned callback count " + count
                    + " exceeds total in-flight occupancy " + occupancy.inFlight());
        }
        occupancy.onEvent(-count);
    }

    /** Step 8's on_event(-1), plus the completed-service cross-check feed. */
    public void requestCompleted(double serviceMs) {
        occupancy.onEvent(-1);
        completedServiceMsInterval.add(serviceMs);
        cumulativeCompletedServiceMs.add(serviceMs);
    }

    /** File one service-time sample into H[level][gapBucket] (step 8 rules at the call site). */
    public void fileServiceTime(int levelIndex, int gapBucket, double serviceMs) {
        histograms.file(levelIndex, gapBucket, serviceMs);
    }

    public ServiceTimeHistograms histograms() {
        return histograms;
    }

    // ===== Control interval =====

    private void closeControlInterval() {
        OccupancyMeter.Interval interval = occupancy.closeInterval();
        double intervalMs = interval.intervalNanos() / 1_000_000.0;
        if (intervalMs <= 0) {
            return;
        }
        double slotMs = interval.slotNanos() / 1_000_000.0;
        double utilization = slotMs / (S_MAX * intervalMs);
        double crossCheck = completedServiceMsInterval.sumThenReset() / (S_MAX * intervalMs);
        cumulativeSlotNanos.addAndGet(interval.slotNanos());
        lastUtilization = utilization;
        lastCrossCheckUtilization = crossCheck;

        // The price moves once per interval; requests read the published
        // lambda, never u.
        priceController.update(utilization);

        String csvPath = "occupancy_" + nodeId + ".csv";
        File file = new File(csvPath);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                out.println("Timestamp,U,CrossCheckU,AvgInFlight,InFlightAtClose,Lambda,Role");
            }
            out.printf("%d,%.6f,%.6f,%.3f,%d,%.8f,%s%n", System.currentTimeMillis(), utilization, crossCheck,
                    interval.averageInFlight(), interval.inFlightAtClose(), priceController.lambda(),
                    roleSupplier.get());
        } catch (IOException e) {
            System.err.println("Failed to write " + csvPath + ": " + e.getMessage());
        }
    }

    // This node's own view of its Raft role, stamped into each occupancy row
    // so the analysis can reconstruct the leadership timeline (including a
    // deposed leader that still believes it leads during a partition).
    private volatile java.util.function.Supplier<String> roleSupplier = () -> "-";

    public void setRoleSupplier(java.util.function.Supplier<String> supplier) {
        this.roleSupplier = supplier;
    }

    /** The shadow price (profit per ms of slot time); up to one interval stale. */
    public double lambda() {
        return priceController.lambda();
    }

    /** Test-only: set the price directly (pair with a near-zero eta so the controller holds it). */
    void forceLambdaForTest(double value) {
        priceController.forceLambda(value);
    }

    /**
     * Reserve a slot on an uncalibrated cell (no samples yet). Callers must
     * release on completion. False = too many requests already riding it.
     */
    public boolean tryAcquireUncalibratedRider(int levelIndex, int gapBucket) {
        if (uncalibratedRiders[levelIndex][gapBucket].incrementAndGet() <= UNCALIBRATED_RIDER_CAP) {
            return true;
        }
        uncalibratedRiders[levelIndex][gapBucket].decrementAndGet();
        return false;
    }

    public void releaseUncalibratedRider(int levelIndex, int gapBucket) {
        uncalibratedRiders[levelIndex][gapBucket].decrementAndGet();
    }

    private void dumpHistograms() {
        String csvPath = "histograms_" + nodeId + ".csv";
        File file = new File(csvPath);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                out.println("Timestamp,Level,GapBucket,DecayedCount,MeanMs,P50Ms,P95Ms,P99Ms");
            }
            long now = System.currentTimeMillis();
            for (int l = 0; l < histograms.numLevels(); l++) {
                for (int g = 0; g < ServiceTimeHistograms.GAP_BUCKETS; g++) {
                    ServiceTimeHistograms.Snapshot snapshot = histograms.snapshot(l, g);
                    if (snapshot.totalCount <= 0) {
                        continue;
                    }
                    out.printf("%d,%s,%d,%.2f,%.4f,%.4f,%.4f,%.4f%n", now, levelLabel(l), g,
                            snapshot.totalCount, snapshot.meanMs,
                            snapshot.quantileMs(0.50), snapshot.quantileMs(0.95), snapshot.quantileMs(0.99));
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to write " + csvPath + ": " + e.getMessage());
        }
    }

    // ===== Introspection (stage 4 and tests) =====

    public double lastUtilization() {
        return lastUtilization;
    }

    public double lastCrossCheckUtilization() {
        return lastCrossCheckUtilization;
    }

    public long cumulativeSlotNanos() {
        return cumulativeSlotNanos.get();
    }

    public double cumulativeCompletedServiceMs() {
        return cumulativeCompletedServiceMs.sum();
    }

    public int inFlight() {
        return occupancy.inFlight();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        drainMetrics.flush();
    }
}
