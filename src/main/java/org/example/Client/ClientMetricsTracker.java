package org.example.Client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Client-side ledger: every cross-arm comparison metric lives here, measured
 * at the client from framed request/response exchanges. Every CSV row is one
 * interval: each cell's counters are snapshotted and reset at every flush
 * (~1 s), so rows are per-interval activity, not running totals, and the
 * P50/P90/P95/P99 columns are that interval's percentiles. Cells with no
 * activity in an interval write no row. The driver's console totals come
 * from separate cumulative counters that never reset.
 *
 * Chosen is what the workload asked for; executed is what the server
 * delivered (fallbacks make them differ). Redirect resends, hard failures,
 * lost requests (no response), and successful responses that miss every
 * applicable SLA deadline are counted per cell.
 *
 * Stage 5 adds the four instrumentation streams: predicted vs realized profit
 * (sums per cell; a misprediction is an accounting error, not just a missed
 * deadline), free vs waiting upgrades above the SLA floor, client-observed
 * latency tails per cell (geometric histogram, P50/P95/P99, not averages),
 * and the satisfied rung per request (counts per rung index, the direct
 * head-to-head metric against Pileus).
 */
public final class ClientMetricsTracker {

    private ClientMetricsTracker() {
    }

    private static final String CSV_PATH = "client_metrics_global.csv";
    private static final long FLUSH_INTERVAL_MS = 1000;

    // Latency histogram: the same geometric bucketing as the server's
    // service-time histograms (64 buckets from 0.5 ms, ratio 1.15) plus one
    // overflow bucket.
    private static final int LATENCY_BUCKETS = 64;
    private static final double LATENCY_BASE_MS = 0.5;
    private static final double LATENCY_RATIO = 1.15;
    private static final double LOG_RATIO = Math.log(LATENCY_RATIO);

    /** SLAs may register at most this many rungs (fixed satisfied-rung columns). */
    public static final int MAX_RUNGS = 4;

    private record Key(int nodeId, String site, String chosen, String executed) {
    }

    private static final class Cell {
        final AtomicLong count = new AtomicLong();
        final AtomicLong transportCalls = new AtomicLong();
        final DoubleAdder transportInvocationLatencySumMs = new DoubleAdder();
        final AtomicLongArray transportInvocationLatencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
        final AtomicLong fallbacks = new AtomicLong();
        final AtomicLong redirects = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final DoubleAdder rejectionTotalLatencySumMs = new DoubleAdder();
        final DoubleAdder rejectionFeedbackLatencySumMs = new DoubleAdder();
        final AtomicLong invalidRejectionFeedbackTimestamps = new AtomicLong();
        final AtomicLongArray rejectionTotalLatencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
        final AtomicLongArray rejectionFeedbackLatencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
        final AtomicLong lost = new AtomicLong();
        final AtomicLong deadlineExceeded = new AtomicLong();
        final AtomicLong violations = new AtomicLong();
        final DoubleAdder latencySumMs = new DoubleAdder();
        final DoubleAdder predictedProfitSum = new DoubleAdder();
        final DoubleAdder realizedProfitSum = new DoubleAdder();
        final AtomicLong upgradesFree = new AtomicLong();
        final AtomicLong upgradesWaiting = new AtomicLong();
        // Index MAX_RUNGS counts responses where no rung was satisfied.
        final AtomicLongArray satisfiedRung = new AtomicLongArray(MAX_RUNGS + 1);
        final AtomicLongArray latencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    }

    private static final ConcurrentHashMap<Key, Cell> cells = new ConcurrentHashMap<>();
    private static final AtomicLong lastFlushMs = new AtomicLong(0);

    // Run-lifetime totals for the driver's progress prints and final report;
    // these never reset, unlike the per-interval cells above.
    private static final AtomicLong runResponses = new AtomicLong();
    private static final AtomicLong runRejected = new AtomicLong();
    private static final AtomicLong runLost = new AtomicLong();
    private static final AtomicLong runViolations = new AtomicLong();
    private static final DoubleAdder runPredictedProfit = new DoubleAdder();
    private static final DoubleAdder runRealizedProfit = new DoubleAdder();

    private static Cell cell(int nodeId, String site, String chosen, String executed) {
        return cells.computeIfAbsent(new Key(nodeId, site, chosen, executed), k -> new Cell());
    }

    private static int latencyBucketOf(double latencyMs) {
        if (latencyMs <= LATENCY_BASE_MS) {
            return 0;
        }
        int index = (int) Math.floor(Math.log(latencyMs / LATENCY_BASE_MS) / LOG_RATIO);
        return Math.min(index, LATENCY_BUCKETS);
    }

    /** Upper edge of a latency bucket, the value quantiles report. */
    private static double latencyBucketUpperMs(int bucket) {
        return LATENCY_BASE_MS * Math.pow(LATENCY_RATIO, bucket + 1.0);
    }

    /**
     * One served response. {@code satisfiedRung} is the client-graded rung
     * index (-1 = none met); {@code upgraded} means the graded delivery was
     * above the SLA's floor, split into free vs waiting by {@code waited}.
     */
    public static void recordResponse(int nodeId, String site, String chosen, String executed, double latencyMs,
            boolean fellBack, boolean deadlineViolation, double predictedProfit, double realizedProfit,
            int satisfiedRung, boolean upgraded, boolean waited) {
        Cell c = cell(nodeId, site, chosen, executed);
        c.count.incrementAndGet();
        c.latencySumMs.add(latencyMs);
        c.latencyBuckets.incrementAndGet(latencyBucketOf(latencyMs));
        c.predictedProfitSum.add(predictedProfit);
        c.realizedProfitSum.add(realizedProfit);
        if (satisfiedRung >= MAX_RUNGS) {
            throw new IllegalArgumentException("satisfiedRung " + satisfiedRung + " exceeds MAX_RUNGS " + MAX_RUNGS);
        }
        c.satisfiedRung.incrementAndGet(satisfiedRung < 0 ? MAX_RUNGS : satisfiedRung);
        if (upgraded) {
            (waited ? c.upgradesWaiting : c.upgradesFree).incrementAndGet();
        }
        if (fellBack) {
            c.fallbacks.incrementAndGet();
        }
        if (deadlineViolation) {
            c.violations.incrementAndGet();
            runViolations.incrementAndGet();
        }
        runResponses.incrementAndGet();
        runPredictedProfit.add(predictedProfit);
        runRealizedProfit.add(realizedProfit);
        maybeFlush();
    }

    public static void recordRedirect(int nodeId, String site, String chosen) {
        cell(nodeId, site, chosen, "-").redirects.incrementAndGet();
        maybeFlush();
    }

    public static void recordFailure(int nodeId, String site, String chosen) {
        cell(nodeId, site, chosen, "-").failures.incrementAndGet();
        maybeFlush();
    }

    // Transport errors by cause, every attempt (retries included), so a run
    // says why its calls failed instead of only how many did. Keyed by the
    // failure description with digits collapsed, which folds addresses,
    // timings and request ids into one key per cause.
    private static final Map<String, AtomicLong> transportErrorCauses = new ConcurrentHashMap<>();
    private static final int CAUSE_KEY_MAX_CHARS = 160;

    // The first few full descriptions per cause: a server-selection timeout,
    // for one, carries the driver's view of every server's state, which the
    // collapsed key drops.
    private static final int CAUSE_SAMPLES = 2;
    private static final int CAUSE_SAMPLE_MAX_CHARS = 3000;
    private static final Map<String, java.util.List<String>> transportErrorSamples = new ConcurrentHashMap<>();

    /** A transport attempt failed with this status description (not a rejection or a deadline). */
    public static void recordTransportError(String description) {
        String key = causeKey(description);
        transportErrorCauses.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
        java.util.List<String> samples = transportErrorSamples.computeIfAbsent(key,
                k -> java.util.Collections.synchronizedList(new java.util.ArrayList<>(CAUSE_SAMPLES)));
        if (samples.size() < CAUSE_SAMPLES && description != null) {
            samples.add(description.length() > CAUSE_SAMPLE_MAX_CHARS
                    ? description.substring(0, CAUSE_SAMPLE_MAX_CHARS) + "..." : description);
        }
    }

    /** The first full descriptions recorded under this cause key, in arrival order. */
    public static java.util.List<String> transportErrorSamples(String causeKey) {
        java.util.List<String> samples = transportErrorSamples.get(causeKey);
        return samples == null ? java.util.List.of() : java.util.List.copyOf(samples);
    }

    /** Normalises a failure description into a low-cardinality cause key. */
    static String causeKey(String description) {
        String key = description == null ? "(no description)" : description.replaceAll("[0-9]+", "#").trim();
        if (key.isEmpty()) {
            key = "(no description)";
        }
        return key.length() > CAUSE_KEY_MAX_CHARS ? key.substring(0, CAUSE_KEY_MAX_CHARS) : key;
    }

    /** Transport error counts by cause, largest first. */
    public static java.util.List<Map.Entry<String, Long>> transportErrorsByCause() {
        java.util.List<Map.Entry<String, Long>> out = new java.util.ArrayList<>();
        for (Map.Entry<String, AtomicLong> e : transportErrorCauses.entrySet()) {
            out.add(Map.entry(e.getKey(), e.getValue().get()));
        }
        out.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
        return out;
    }

    /** A request or retry was submitted to the asynchronous framed transport. */
    public static void recordTransportCall(int nodeId, String site, String chosen, double invocationMs) {
        Cell c = cell(nodeId, site, chosen, "-");
        c.transportCalls.incrementAndGet();
        c.transportInvocationLatencySumMs.add(invocationMs);
        c.transportInvocationLatencyBuckets.incrementAndGet(latencyBucketOf(invocationMs));
        maybeFlush();
    }

    /** The per-RPC SLA deadline expired. It is a violation, not a lost request. */
    public static void recordDeadlineExceeded(int nodeId, String site, String chosen) {
        Cell c = cell(nodeId, site, chosen, "-");
        c.deadlineExceeded.incrementAndGet();
        c.violations.incrementAndGet();
        runViolations.incrementAndGet();
        maybeFlush();
    }

    /** Admission control shed the request; not retried by design. */
    public static void recordRejected(int nodeId, String site, String chosen, double totalLatencyMs,
            long serverReplyEpochMs, long clientReceiveEpochMs) {
        Cell c = cell(nodeId, site, chosen, "-");
        c.rejected.incrementAndGet();
        c.rejectionTotalLatencySumMs.add(totalLatencyMs);
        c.rejectionTotalLatencyBuckets.incrementAndGet(latencyBucketOf(totalLatencyMs));
        if (serverReplyEpochMs <= 0 || clientReceiveEpochMs < serverReplyEpochMs) {
            c.invalidRejectionFeedbackTimestamps.incrementAndGet();
        } else {
            double feedbackLatencyMs = clientReceiveEpochMs - serverReplyEpochMs;
            c.rejectionFeedbackLatencySumMs.add(feedbackLatencyMs);
            c.rejectionFeedbackLatencyBuckets.incrementAndGet(latencyBucketOf(feedbackLatencyMs));
        }
        runRejected.incrementAndGet();
        maybeFlush();
    }

    public static void recordLost(int nodeId, String site, String chosen) {
        cell(nodeId, site, chosen, "-").lost.incrementAndGet();
        runLost.incrementAndGet();
        maybeFlush();
    }

    public static long totalRejected() {
        return runRejected.get();
    }

    public static long totalViolations() {
        return runViolations.get();
    }

    public static long totalResponses() {
        return runResponses.get();
    }

    public static long totalLost() {
        return runLost.get();
    }

    public static double totalPredictedProfit() {
        return runPredictedProfit.sum();
    }

    public static double totalRealizedProfit() {
        return runRealizedProfit.sum();
    }

    private static double quantileMs(long[] buckets, long total, double quantile) {
        if (total == 0) {
            return 0.0;
        }
        long target = (long) Math.ceil(quantile * total);
        long cumulative = 0;
        for (int i = 0; i <= LATENCY_BUCKETS; i++) {
            cumulative += buckets[i];
            if (cumulative >= target) {
                return latencyBucketUpperMs(i);
            }
        }
        return latencyBucketUpperMs(LATENCY_BUCKETS);
    }

    private static void maybeFlush() {
        long now = System.currentTimeMillis();
        long last = lastFlushMs.get();
        if (now - last >= FLUSH_INTERVAL_MS && lastFlushMs.compareAndSet(last, now)) {
            flush(now);
        }
    }

    /** Force a flush (end of run). */
    public static void flushNow() {
        flush(System.currentTimeMillis());
    }

    /**
     * Write one row per cell with activity this interval, resetting each
     * counter as it is read. Concurrent increments land in exactly one
     * interval (getAndSet/sumThenReset), though a response racing the flush
     * may split its fields across two adjacent rows - harmless at metric
     * granularity.
     */
    private static synchronized void flush(long now) {
        File file = new File(CSV_PATH);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                StringBuilder header = new StringBuilder(
                        "Timestamp,NodeId,Site,ChosenLevel,ExecutedLevel,Count,AvgLatencyMs,"
                        + "P50Ms,P90Ms,P95Ms,P99Ms,PredictedProfitSum,RealizedProfitSum,"
                        + "UpgradesFree,UpgradesWaiting,"
                        + "SatisfiedRung0,SatisfiedRung1,SatisfiedRung2,SatisfiedRung3,SatisfiedNone,"
                        + "Fallbacks,Redirects,Failures,TransportCalls,AvgTransportInvocationMs,P99TransportInvocationMs,"
                        + "Rejected,AvgRejectTotalMs,P50RejectTotalMs,"
                        + "P90RejectTotalMs,P99RejectTotalMs,AvgRejectFeedbackMs,P50RejectFeedbackMs,"
                        + "P90RejectFeedbackMs,P99RejectFeedbackMs,InvalidRejectFeedbackTimestamps,"
                        + "Lost,DeadlineExceeded,Violations");
                // Raw client-latency histogram (served responses), one column
                // per geometric bucket: bucket i's upper edge is
                // 0.5 * 1.15^(i+1) ms, the last bucket is the overflow.
                // Unlike the percentile columns these are additive, so any
                // later aggregation (per phase, site, arm, or whole run) can
                // sum the vectors and read exact pooled percentiles.
                for (int i = 0; i <= LATENCY_BUCKETS; i++) {
                    header.append(",LatBucket").append(i);
                }
                out.println(header);
            }
            for (Map.Entry<Key, Cell> e : cells.entrySet()) {
                Key k = e.getKey();
                Cell c = e.getValue();
                long count = c.count.getAndSet(0);
                long transportCalls = c.transportCalls.getAndSet(0);
                double transportInvocationLatencySum = c.transportInvocationLatencySumMs.sumThenReset();
                long fallbacks = c.fallbacks.getAndSet(0);
                long redirects = c.redirects.getAndSet(0);
                long failures = c.failures.getAndSet(0);
                long rejected = c.rejected.getAndSet(0);
                double rejectionTotalLatencySum = c.rejectionTotalLatencySumMs.sumThenReset();
                double rejectionFeedbackLatencySum = c.rejectionFeedbackLatencySumMs.sumThenReset();
                long invalidRejectionFeedbackTimestamps = c.invalidRejectionFeedbackTimestamps.getAndSet(0);
                long lost = c.lost.getAndSet(0);
                long deadlineExceeded = c.deadlineExceeded.getAndSet(0);
                long violations = c.violations.getAndSet(0);
                double latencySum = c.latencySumMs.sumThenReset();
                double predicted = c.predictedProfitSum.sumThenReset();
                double realized = c.realizedProfitSum.sumThenReset();
                long upFree = c.upgradesFree.getAndSet(0);
                long upWaiting = c.upgradesWaiting.getAndSet(0);
                long[] rungs = new long[MAX_RUNGS + 1];
                for (int i = 0; i <= MAX_RUNGS; i++) {
                    rungs[i] = c.satisfiedRung.getAndSet(i, 0);
                }
                long[] buckets = new long[LATENCY_BUCKETS + 1];
                long bucketTotal = 0;
                for (int i = 0; i <= LATENCY_BUCKETS; i++) {
                    buckets[i] = c.latencyBuckets.getAndSet(i, 0);
                    bucketTotal += buckets[i];
                }
                long[] rejectionTotalBuckets = new long[LATENCY_BUCKETS + 1];
                long rejectionTotalBucketCount = 0;
                long[] rejectionFeedbackBuckets = new long[LATENCY_BUCKETS + 1];
                long rejectionFeedbackBucketCount = 0;
                long[] transportInvocationBuckets = new long[LATENCY_BUCKETS + 1];
                long transportInvocationBucketCount = 0;
                for (int i = 0; i <= LATENCY_BUCKETS; i++) {
                    transportInvocationBuckets[i] = c.transportInvocationLatencyBuckets.getAndSet(i, 0);
                    transportInvocationBucketCount += transportInvocationBuckets[i];
                    rejectionTotalBuckets[i] = c.rejectionTotalLatencyBuckets.getAndSet(i, 0);
                    rejectionTotalBucketCount += rejectionTotalBuckets[i];
                    rejectionFeedbackBuckets[i] = c.rejectionFeedbackLatencyBuckets.getAndSet(i, 0);
                    rejectionFeedbackBucketCount += rejectionFeedbackBuckets[i];
                }
                if (count == 0 && transportCalls == 0 && fallbacks == 0 && redirects == 0 && failures == 0
                        && rejected == 0 && lost == 0 && violations == 0) {
                    continue; // no activity this interval
                }
                double avg = count == 0 ? 0.0 : latencySum / count;
                StringBuilder bucketCols = new StringBuilder();
                for (int i = 0; i <= LATENCY_BUCKETS; i++) {
                    bucketCols.append(',').append(buckets[i]);
                }
                out.printf("%d,%d,%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
                                + "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%d,"
                                + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d%s%n",
                        now, k.nodeId(), k.site(), k.chosen(), k.executed(), count, avg,
                        quantileMs(buckets, bucketTotal, 0.50), quantileMs(buckets, bucketTotal, 0.90),
                        quantileMs(buckets, bucketTotal, 0.95),
                        quantileMs(buckets, bucketTotal, 0.99),
                        predicted, realized,
                        upFree, upWaiting,
                        rungs[0], rungs[1], rungs[2], rungs[3], rungs[MAX_RUNGS],
                        fallbacks, redirects, failures, transportCalls,
                        transportCalls == 0 ? 0.0 : transportInvocationLatencySum / transportCalls,
                        quantileMs(transportInvocationBuckets, transportInvocationBucketCount, 0.99),
                        rejected,
                        rejected == 0 ? 0.0 : rejectionTotalLatencySum / rejected,
                        quantileMs(rejectionTotalBuckets, rejectionTotalBucketCount, 0.50),
                        quantileMs(rejectionTotalBuckets, rejectionTotalBucketCount, 0.90),
                        quantileMs(rejectionTotalBuckets, rejectionTotalBucketCount, 0.99),
                        rejectionFeedbackBucketCount == 0
                                ? 0.0 : rejectionFeedbackLatencySum / rejectionFeedbackBucketCount,
                        quantileMs(rejectionFeedbackBuckets, rejectionFeedbackBucketCount, 0.50),
                        quantileMs(rejectionFeedbackBuckets, rejectionFeedbackBucketCount, 0.90),
                        quantileMs(rejectionFeedbackBuckets, rejectionFeedbackBucketCount, 0.99),
                        invalidRejectionFeedbackTimestamps, lost, deadlineExceeded, violations,
                        bucketCols);
            }
        } catch (IOException e) {
            System.err.println("Failed to write " + CSV_PATH + ": " + e.getMessage());
        }
    }
}
