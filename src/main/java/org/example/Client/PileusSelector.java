package org.example.Client;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLongArray;

import org.example.Utility.RungScorer;
import org.example.raft.ReadLevel;

/**
 * The Pileus client: picks (contact server, target rung) to maximize expected
 * profit, confirmed after the fact by grading. Per candidate the expectation
 * is profit x P(consistency feasible at that server) x P(latency within the
 * rung's threshold):
 *
 * - Consistency is binary from tracked per-server high-water indices (every
 *   response reports the server's log and commit index) against the session's
 *   session index (its causal frontier); eventual rungs are always feasible, linearizable is structural
 *   (leader, or any server when follower LIN reads are enabled) because the
 *   mechanism guarantees the outcome - all its uncertainty is latency.
 * - Latency comes from 2n + wc response-time sliding windows: per-server
 *   plain reads, per-server linearizable reads (the confirmation round and
 *   catch-up wait are part of what must be predicted), and per-concern
 *   writes (writes always go to the leader). Empty windows read as feasible
 *   (cold-start optimism), and timed-out attempts land at-or-beyond the
 *   threshold so they count themselves as misses.
 *
 * A small exploration fraction routes to a random server so windows and
 * staleness estimates never freeze on stale samples. Ties break by rung
 * registration order (the SLA lists rungs in decreasing preference, the
 * paper's subSLA ordering), so an all-zero expectation - cold start and
 * overload look identical here - still targets the most preferred rung
 * rather than the cheapest promise.
 *
 * Two signals extend the paper's model for this testbed's bounded-wait
 * mechanics (a wait that expires falls back below the target, an outcome
 * the original protocol cannot produce):
 * - {@link Choice#feasible}: whether the chosen rung's consistency was
 *   attainable under the staleness estimates. The session suppresses the
 *   server-side wait for knowingly-infeasible targets instead of burning
 *   occupancy on a wait it already predicted cannot succeed.
 * - Delivery rates: a decayed per-(server, level) fraction of targeted
 *   reads that actually graded at or above their target. It multiplies into
 *   expected profit like pAdmit, so a server that keeps falling back below
 *   the target (e.g. commit lag it cannot cover within the wait clamp)
 *   loses attractiveness even while its latency window looks healthy. The
 *   prior keeps cold cells neutral and decay restores optimism, so a
 *   condemned (server, level) can win its way back.
 */
final class PileusSelector {

    private final int numServers;
    private final int majority;
    private final SlidingWindow[] plainWindows;
    private final SlidingWindow[] linWindows;
    private final SlidingWindow[] writeWindows; // index = concern - 1
    private final AtomicLongArray highLogIndex;
    private final AtomicLongArray highCommitIndex;
    private final boolean followerLinReads;
    private final double explorationFraction;
    private final Random random;

    // Delivery-rate feedback (reads): decayed per-(node, target level)
    // success/attempt counts, guarded by their own lock (AdmitRates pattern).
    private static final double DELIVERY_PRIOR = 1.0;
    private final double[][] deliverySuccesses;
    private final double[][] deliveryAttempts;

    record Choice(int node, int rungIndex, double expectedProfit, boolean feasible) {
    }

    PileusSelector(int numServers, int majority, int windowSize, boolean followerLinReads,
            double explorationFraction, Random random) {
        this.numServers = numServers;
        this.majority = majority;
        this.followerLinReads = followerLinReads;
        this.explorationFraction = explorationFraction;
        this.random = random;
        this.plainWindows = new SlidingWindow[numServers];
        this.linWindows = new SlidingWindow[numServers];
        for (int i = 0; i < numServers; i++) {
            plainWindows[i] = new SlidingWindow(windowSize);
            linWindows[i] = new SlidingWindow(windowSize);
        }
        this.writeWindows = new SlidingWindow[majority];
        for (int c = 0; c < majority; c++) {
            writeWindows[c] = new SlidingWindow(windowSize);
        }
        this.highLogIndex = new AtomicLongArray(numServers);
        this.highCommitIndex = new AtomicLongArray(numServers);
        for (int i = 0; i < numServers; i++) {
            highLogIndex.set(i, -1);
            highCommitIndex.set(i, -1);
        }
        int levels = ReadLevel.LINEARIZABLE.getNumber() + 1;
        this.deliverySuccesses = new double[numServers][levels];
        this.deliveryAttempts = new double[numServers][levels];
    }

    // ===== Observation =====

    void observeIndices(int node, long logIndex, long commitIndex) {
        highLogIndex.accumulateAndGet(node, logIndex, Math::max);
        highCommitIndex.accumulateAndGet(node, commitIndex, Math::max);
    }

    void observeRead(int node, boolean linTargeted, double latencyMs) {
        (linTargeted ? linWindows : plainWindows)[node].add(latencyMs);
    }

    void observeWrite(int concern, double latencyMs) {
        writeWindows[Math.min(Math.max(1, concern), majority) - 1].add(latencyMs);
    }

    // A rejection carries no latency, but it must not be invisible: an empty
    // window reads as cold-start optimistic, so a node rejecting under its
    // occupancy cap would otherwise stay the most attractive target forever.
    // A penalty sample beyond every threshold makes the window reflect the
    // rejection; exploration re-probes the node once it recovers.
    private static final double REJECTION_PENALTY_MS = 1e9;

    void observeReadRejected(int node, boolean linTargeted) {
        observeRead(node, linTargeted, REJECTION_PENALTY_MS);
    }

    void observeWriteRejected(int concern) {
        observeWrite(concern, REJECTION_PENALTY_MS);
    }

    /**
     * A targeted read was served and graded: {@code delivered} says whether
     * the graded level reached the target. In pileus mode the target is the
     * rung the client asked for; in chameleonPileus mode it is the rung the
     * selector routed for while the server chose the level, so the selector
     * learns what each server actually delivers for it.
     */
    void observeReadDelivery(int node, int targetStrength, boolean delivered) {
        if (targetStrength < 0 || targetStrength >= deliveryAttempts[node].length) {
            return;
        }
        synchronized (deliveryAttempts) {
            deliveryAttempts[node][targetStrength] += 1;
            if (delivered) {
                deliverySuccesses[node][targetStrength] += 1;
            }
        }
    }

    /** Called on the session's 1 s sweeper tick; gamma is per second. */
    void decayDeliveryRates(double gamma) {
        synchronized (deliveryAttempts) {
            for (int n = 0; n < deliveryAttempts.length; n++) {
                for (int s = 0; s < deliveryAttempts[n].length; s++) {
                    deliveryAttempts[n][s] *= gamma;
                    deliverySuccesses[n][s] *= gamma;
                }
            }
        }
    }

    /** P(a read targeted at this level on this node grades at the target). */
    private double pDeliver(int node, int targetStrength) {
        if (targetStrength < 0 || targetStrength >= deliveryAttempts[node].length) {
            return 1.0;
        }
        synchronized (deliveryAttempts) {
            return (deliverySuccesses[node][targetStrength] + DELIVERY_PRIOR)
                    / (deliveryAttempts[node][targetStrength] + DELIVERY_PRIOR);
        }
    }

    // ===== Selection =====

    /**
     * Admission-aware routing passes the requesting SLA's admit tracker
     * (null when disabled): expected profit is multiplied by the node's
     * decayed admit probability for that SLA, so a rejecting node loses
     * attractiveness in profit space instead of through latency-window
     * penalty samples.
     */
    Choice chooseRead(List<RungScorer.Rung> sla, long sessionIndex, int leaderHint,
            AdmitRates admitRates) {
        if (explorationFraction > 0 && random.nextDouble() < explorationFraction) {
            int node = random.nextInt(numServers);
            return bestRungForNode(sla, node, sessionIndex, leaderHint, admitRates);
        }
        Choice best = null;
        for (int node = 0; node < numServers; node++) {
            Choice candidate = bestRungForNode(sla, node, sessionIndex, leaderHint, admitRates);
            if (candidate == null) {
                continue;
            }
            if (best == null || candidate.expectedProfit() > best.expectedProfit()) {
                best = candidate;
            }
        }
        return best;
    }

    /** The best rung to target at one specific server (used for exploration too). */
    private Choice bestRungForNode(List<RungScorer.Rung> sla, int node, long sessionIndex,
            int leaderHint, AdmitRates admitRates) {
        double admitFactor = admitRates == null ? 1.0 : admitRates.pAdmit(node);
        Choice best = null;
        for (int i = 0; i < sla.size(); i++) {
            RungScorer.Rung rung = sla.get(i);
            boolean lin = rung.strength() == ReadLevel.LINEARIZABLE.getNumber();
            if (lin && !followerLinReads && leaderHint >= 0 && node != leaderHint) {
                continue; // LIN is structurally impossible off-leader
            }
            boolean feasible = readFeasible(rung.strength(), node, sessionIndex);
            SlidingWindow window = lin ? linWindows[node] : plainWindows[node];
            double expected = rung.profit() * (feasible ? 1.0 : 0.0)
                    * window.fractionAtMost(rung.thresholdMs()) * admitFactor
                    * pDeliver(node, rung.strength());
            // Strictly-greater keeps the earliest rung on ties: rungs are
            // registered in decreasing preference (the paper's subSLA order),
            // so an all-zero expectation - cold start and overload despair
            // look the same here - still targets the most preferred rung.
            if (best == null || expected > best.expectedProfit()) {
                best = new Choice(node, i, expected, feasible);
            }
        }
        return best;
    }

    private boolean readFeasible(int strength, int node, long sessionIndex) {
        if (strength == ReadLevel.CAUSAL_LOCAL.getNumber()) {
            return sessionIndex < 0 || highLogIndex.get(node) >= sessionIndex;
        }
        if (strength == ReadLevel.CAUSAL_MAJORITY.getNumber()) {
            return sessionIndex < 0 || highCommitIndex.get(node) >= sessionIndex;
        }
        // Eventual levels always; linearizable by mechanism where it may run.
        return true;
    }

    /** Writes always target the leader; only the concern is chosen. */
    Choice chooseWrite(List<RungScorer.Rung> sla, int leaderNode) {
        Choice best = null;
        for (int i = 0; i < sla.size(); i++) {
            RungScorer.Rung rung = sla.get(i);
            int concern = Math.min(Math.max(1, rung.strength()), majority);
            double expected = rung.profit() * writeWindows[concern - 1].fractionAtMost(rung.thresholdMs());
            // Strictly-greater keeps the earliest rung on ties (registration
            // order is decreasing preference), mirroring the read path. A
            // write concern is always attainable - an expired ack wait still
            // delivers whatever replication was reached - so there is no
            // feasibility gate here.
            if (best == null || expected > best.expectedProfit()) {
                best = new Choice(leaderNode, i, expected, true);
            }
        }
        return best;
    }
}
