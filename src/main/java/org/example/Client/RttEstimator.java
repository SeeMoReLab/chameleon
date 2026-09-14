package org.example.Client;

/**
 * Per-node RTT estimation (stage 5, protocol rho): RTT = end-to-end latency
 * minus the server-reported service time. Service time spans frame receipt
 * to reply write on the server's monotonic clock, so it already contains any
 * admission queueing, replication wait or ReadIndex round; the difference is
 * the network legs plus client-side transport time whether the request
 * waited or not, and every reply is a sample. Rejections qualify too (score
 * and reply) and must be fed: they are the only samples still flowing when a
 * node rejects everything, and without them an estimate poisoned by an
 * overload burst could never recover. The estimate is the median of a sliding window
 * per node; empty windows report 0 (optimistic cold start, consistent with
 * the scorer's free-and-certain rule for empty cells).
 */
public final class RttEstimator {

    private final SlidingWindow[] windows;

    public RttEstimator(int numNodes, int windowSize) {
        this.windows = new SlidingWindow[numNodes];
        for (int i = 0; i < numNodes; i++) {
            windows[i] = new SlidingWindow(windowSize);
        }
    }

    /**
     * Feed one reply. Both inputs are durations on their own host's clock, so
     * there is no skew to correct; a negative difference (timer jitter) is
     * discarded outright.
     */
    public void observe(int nodeId, double latencyMs, double serviceTimeMs) {
        double rtt = latencyMs - serviceTimeMs;
        if (rtt >= 0) {
            windows[nodeId].add(rtt);
        }
    }

    /** The current estimate for a node; 0 until a sample arrives. */
    public double estimateMs(int nodeId) {
        return windows[nodeId].medianOr(0.0);
    }

    public int sampleCount(int nodeId) {
        return windows[nodeId].size();
    }
}
