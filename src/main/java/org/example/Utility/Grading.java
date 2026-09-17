package org.example.Utility;

import java.util.List;

import org.example.raft.ReadLevel;

/**
 * Step 7: grade what was actually delivered, not what was targeted. Reads
 * frequently satisfy a stronger level than the one chosen, and that profit is
 * free.
 *
 * The graded strength is the strongest level on the ladder whose own
 * requirement the delivered result demonstrably meets. The executed level is
 * diagnostic rather than an automatic floor, except that a linearizable label
 * proves the ReadIndex mechanism that indices alone cannot reconstruct:
 *
 * - eventual-majority: the returned value's index is at or below the commit
 *   index, so the value is majority-committed (a committed-view read
 *   satisfies this by construction).
 * - causal-local: the view that served the value had reached the session
 *   index (the session's causal frontier), so everything the session had
 *   written or read was visible.
 * - causal-majority: the value is committed and the committed view had
 *   reached the session index, the same frontier causal-local uses; this is
 *   what makes causal-majority strictly stronger than causal-local.
 * - linearizable: never grantable after the fact; it requires the leadership
 *   confirmation round that only the executed path performs.
 *
 * Realized profit is the highest-profit rung whose consistency requirement
 * the graded strength covers and whose threshold the total time, service time
 * plus rho, came in under. Writes cannot be upgraded this way (once
 * acknowledged the decision is spent), so their graded strength is simply the
 * replication count at acknowledgment.
 */
public final class Grading {

    private Grading() {
    }

    /** The best rung met: its index in the SLA's registered rung list, and its profit. */
    public record Realized(int rungIndex, double profit) {
        public static final Realized NONE = new Realized(-1, 0.0);
    }

    /**
     * Grade a read. {@code viewFrontierIndex} is the index frontier of the
     * view that served the value: the last log index for local-view levels,
     * the commit index for committed-view levels. Indices are the node's at
     * grading time; {@code sessionIndex} is the client session's causal
     * frontier, the highest log index it has written or read (-1 = none),
     * for both local and majority views.
     */
    public static int gradeRead(ReadLevel executed, long valueIndex, long commitIndex, long viewFrontierIndex,
            long sessionIndex) {
        int graded = ReadLevel.EVENTUAL_LOCAL.getNumber();
        boolean valueCommitted = valueIndex <= commitIndex;
        boolean viewCoversFrontier = sessionIndex < 0 || viewFrontierIndex >= sessionIndex;
        boolean commitCoversFrontier = sessionIndex < 0 || commitIndex >= sessionIndex;
        if (valueCommitted) {
            graded = Math.max(graded, ReadLevel.EVENTUAL_MAJORITY.getNumber());
        }
        if (viewCoversFrontier) {
            graded = Math.max(graded, ReadLevel.CAUSAL_LOCAL.getNumber());
        }
        // A causal-majority view must make everything the session has
        // observed visible once committed. The same client-owned frontier
        // therefore gates both causal levels.
        if (valueCommitted && commitCoversFrontier) {
            graded = Math.max(graded, ReadLevel.CAUSAL_MAJORITY.getNumber());
        }
        if (executed == ReadLevel.LINEARIZABLE && valueCommitted && commitCoversFrontier) {
            graded = ReadLevel.LINEARIZABLE.getNumber();
        }
        return graded;
    }

    /**
     * The highest-profit rung whose requirement {@code gradedStrength} covers
     * and whose threshold {@code totalMs} (service time plus rho) came in
     * under. Ties on profit go to the earliest registered rung.
     */
    public static Realized realize(List<RungScorer.Rung> slaRungs, int gradedStrength, double totalMs) {
        int bestIndex = -1;
        double bestProfit = 0.0;
        for (int i = 0; i < slaRungs.size(); i++) {
            RungScorer.Rung rung = slaRungs.get(i);
            if (gradedStrength >= rung.strength() && totalMs <= rung.thresholdMs()
                    && rung.profit() > bestProfit) {
                bestIndex = i;
                bestProfit = rung.profit();
            }
        }
        return bestIndex < 0 ? Realized.NONE : new Realized(bestIndex, bestProfit);
    }

    /** True when at least one rung's consistency is satisfied but all such deadlines were missed. */
    public static boolean missedDeadline(List<RungScorer.Rung> slaRungs, int gradedStrength, double totalMs) {
        boolean hasApplicableRung = false;
        for (RungScorer.Rung rung : slaRungs) {
            if (gradedStrength >= rung.strength()) {
                hasApplicableRung = true;
                if (totalMs <= rung.thresholdMs()) {
                    return false;
                }
            }
        }
        return hasApplicableRung;
    }
}
