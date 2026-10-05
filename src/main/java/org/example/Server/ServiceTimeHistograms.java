package org.example.Server;

import java.util.Arrays;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Per-node service-time histograms, one cell per (level, gap bucket).
 *
 * Cells use fixed geometric latency buckets (64 buckets from 0.5 ms, ratio
 * 1.15), so the bucket for a value is a formula, not a search. Each cell also
 * keeps a running sum and count; the mean (omega, the occupancy cost) comes
 * from sum over count, never from bucket midpoints.
 *
 * The hot path files samples into DoubleAdders and never blocks. A periodic
 * refresh tick (single-threaded caller) folds pending samples into the decayed
 * state - state = (state + new) * decay - rebuilds the cumulative arrays, and
 * publishes immutable snapshots. Requests read the published snapshot and
 * never see a partially rebuilt histogram; the snapshot is up to one tick
 * stale by design.
 *
 * With decay d per tick of length t, the effective memory is roughly
 * t / (1 - d): 100 ms and 0.95 gives about 2 seconds. The decay comes from
 * config (server.histogramDecay). That memory holds only while samples
 * arrive: decay rescales every bucket alike, so a cell nobody samples keeps
 * its shape, and the scorer samples only the level it chooses. A level that
 * once looked bad would never be tried again. So a cell is emptied once even
 * its newest sample has decayed to STALE_WEIGHT of its weight (90 ticks, about
 * 9 s, at 0.95): it is uncalibrated again, and the cold-start rider cap probes
 * it. A cell that keeps receiving samples, however rarely, keeps its evidence.
 */
public final class ServiceTimeHistograms {

    public static final int LATENCY_BUCKETS = 64;
    public static final double MIN_LATENCY_MS = 0.5;
    public static final double BUCKET_RATIO = 1.15;
    // A cell is forgotten once its newest sample weighs this little.
    private static final double STALE_WEIGHT = 0.01;

    // Gap buckets: <= 0 (already satisfiable), two coarse waiting bands sized
    // to the configured replication-batch scale, and far behind.
    // Everything at or below zero lands in one bucket whose observed waits are
    // near zero, so "the upgrade is free" emerges from data, not a rule.
    public static final int GAP_BUCKETS = 4;
    private static final long GAP_EDGE_1 = 2000;
    private static final long GAP_EDGE_2 = 20000;

    private static final double LOG_RATIO = Math.log(BUCKET_RATIO);

    private final int numLevels;
    private final double decay;
    // Ticks without a sample after which a cell is emptied.
    final int staleTicks;

    // Hot path: lock-free pending samples.
    private final DoubleAdder[][][] pendingBuckets;
    private final DoubleAdder[][] pendingSum;
    private final DoubleAdder[][] pendingCount;

    // Owned by the refresh tick thread.
    private final double[][][] stateBuckets;
    private final double[][] stateSum;
    private final double[][] stateCount;
    private final int[][] idleTicks;

    private volatile Snapshot[][] snapshots;

    public ServiceTimeHistograms(int numLevels, double decay) {
        if (!(decay > 0) || !(decay < 1)) {
            throw new IllegalArgumentException("histogram decay must be in (0, 1), got " + decay);
        }
        this.numLevels = numLevels;
        this.decay = decay;
        this.staleTicks = (int) Math.ceil(Math.log(STALE_WEIGHT) / Math.log(decay));
        this.idleTicks = new int[numLevels][GAP_BUCKETS];
        this.pendingBuckets = new DoubleAdder[numLevels][GAP_BUCKETS][LATENCY_BUCKETS];
        this.pendingSum = new DoubleAdder[numLevels][GAP_BUCKETS];
        this.pendingCount = new DoubleAdder[numLevels][GAP_BUCKETS];
        this.stateBuckets = new double[numLevels][GAP_BUCKETS][LATENCY_BUCKETS];
        this.stateSum = new double[numLevels][GAP_BUCKETS];
        this.stateCount = new double[numLevels][GAP_BUCKETS];
        Snapshot[][] empty = new Snapshot[numLevels][GAP_BUCKETS];
        for (int l = 0; l < numLevels; l++) {
            for (int g = 0; g < GAP_BUCKETS; g++) {
                for (int b = 0; b < LATENCY_BUCKETS; b++) {
                    pendingBuckets[l][g][b] = new DoubleAdder();
                }
                pendingSum[l][g] = new DoubleAdder();
                pendingCount[l][g] = new DoubleAdder();
                empty[l][g] = new Snapshot(new double[LATENCY_BUCKETS], 0.0, 0.0);
            }
        }
        this.snapshots = empty;
    }

    public int numLevels() {
        return numLevels;
    }

    /** Latency bucket b covers [lowerEdge(b), upperEdge(b)); bucket 0 starts at 0. */
    public static int latencyBucketOf(double ms) {
        if (ms <= MIN_LATENCY_MS) {
            return 0;
        }
        int index = (int) Math.floor(Math.log(ms / MIN_LATENCY_MS) / LOG_RATIO);
        return Math.min(index, LATENCY_BUCKETS - 1);
    }

    static double lowerEdgeMs(int bucket) {
        return bucket == 0 ? 0.0 : MIN_LATENCY_MS * Math.pow(BUCKET_RATIO, bucket);
    }

    static double upperEdgeMs(int bucket) {
        return MIN_LATENCY_MS * Math.pow(BUCKET_RATIO, bucket + 1);
    }

    public static int gapBucketOf(long gap) {
        if (gap <= 0) {
            return 0;
        }
        if (gap <= GAP_EDGE_1) {
            return 1;
        }
        if (gap <= GAP_EDGE_2) {
            return 2;
        }
        return 3;
    }

    /** File one sample: three operations, no rebuild (step 8). */
    public void file(int level, int gapBucket, double serviceMs) {
        pendingBuckets[level][gapBucket][latencyBucketOf(serviceMs)].add(1.0);
        pendingSum[level][gapBucket].add(serviceMs);
        pendingCount[level][gapBucket].add(1.0);
    }

    /**
     * Fold pending samples into the decayed state and publish fresh
     * snapshots. Must be called from a single thread (the refresh scheduler).
     */
    public void refreshTick() {
        Snapshot[][] fresh = new Snapshot[numLevels][GAP_BUCKETS];
        for (int l = 0; l < numLevels; l++) {
            for (int g = 0; g < GAP_BUCKETS; g++) {
                double[] state = stateBuckets[l][g];
                double[] cumulative = new double[LATENCY_BUCKETS];
                double running = 0.0;
                for (int b = 0; b < LATENCY_BUCKETS; b++) {
                    state[b] = (state[b] + pendingBuckets[l][g][b].sumThenReset()) * decay;
                    running += state[b];
                    cumulative[b] = running;
                }
                stateSum[l][g] = (stateSum[l][g] + pendingSum[l][g].sumThenReset()) * decay;
                double arrived = pendingCount[l][g].sumThenReset();
                stateCount[l][g] = (stateCount[l][g] + arrived) * decay;
                idleTicks[l][g] = arrived > 0 ? 0 : Math.min(idleTicks[l][g] + 1, staleTicks);
                if (idleTicks[l][g] >= staleTicks && stateCount[l][g] > 0) {
                    Arrays.fill(state, 0.0);
                    Arrays.fill(cumulative, 0.0);
                    stateSum[l][g] = 0.0;
                    stateCount[l][g] = 0.0;
                }
                double mean = stateCount[l][g] <= 0 ? 0.0 : stateSum[l][g] / stateCount[l][g];
                fresh[l][g] = new Snapshot(cumulative, stateCount[l][g], mean);
            }
        }
        snapshots = fresh;
    }

    public Snapshot snapshot(int level, int gapBucket) {
        return snapshots[level][gapBucket];
    }

    /** Immutable published view of one cell. */
    public static final class Snapshot {
        private final double[] cumulative; // cumulative decayed counts per latency bucket
        public final double totalCount;
        /** Mean service time (omega, the occupancy cost); 0 when empty. */
        public final double meanMs;

        Snapshot(double[] cumulative, double totalCount, double meanMs) {
            this.cumulative = cumulative;
            this.totalCount = totalCount;
            this.meanMs = meanMs;
        }

        /**
         * F(x): fraction of recent samples that finished within x ms, with
         * linear interpolation inside the bucket containing x. An empty cell
         * is optimistically certain (1.0): the uncalibrated level is treated
         * as free until samples arrive (step 4 cold-start rule).
         */
        public double fractionAtMost(double ms) {
            if (totalCount <= 0) {
                return 1.0;
            }
            if (ms <= 0) {
                return 0.0;
            }
            int bucket = latencyBucketOf(ms);
            double below = bucket == 0 ? 0.0 : cumulative[bucket - 1];
            double inBucket = cumulative[bucket] - below;
            double lo = lowerEdgeMs(bucket);
            double hi = upperEdgeMs(bucket);
            double fraction = Math.min(1.0, Math.max(0.0, (ms - lo) / (hi - lo)));
            return Math.min(1.0, (below + inBucket * fraction) / totalCount);
        }

        /** Approximate quantile in ms (interpolated); 0 when the cell is empty. */
        public double quantileMs(double q) {
            if (totalCount <= 0) {
                return 0.0;
            }
            double target = q * totalCount;
            for (int b = 0; b < LATENCY_BUCKETS; b++) {
                if (cumulative[b] >= target) {
                    double below = b == 0 ? 0.0 : cumulative[b - 1];
                    double inBucket = cumulative[b] - below;
                    double fraction = inBucket <= 0 ? 0.0 : (target - below) / inBucket;
                    return lowerEdgeMs(b) + (upperEdgeMs(b) - lowerEdgeMs(b)) * fraction;
                }
            }
            return upperEdgeMs(LATENCY_BUCKETS - 1);
        }
    }
}
