package org.example.Client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.Utility.RungScorer;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;

import io.grpc.Status;

/**
 * One logical application session against the cluster. It shares an
 * asynchronous framed transport with the other sessions and owns the
 * session index (the session's causal frontier: the highest log index of
 * anything it has written or read), a per-node RTT estimator fed by no-wait
 * replies, and the client
 * side of the experiment arm ({@link ClientMode}):
 *
 * - chameleon: reads to the lowest-RTT node, the server scorer decides.
 * - chameleonPileus: reads routed by the Pileus selector, server decides.
 * - pileus: the selector picks (server, rung); the request carries the
 *   target (wantLinearizable / requestedWriteConcern) plus a wait bound of
 *   threshold minus RTT, the client's d_max analog.
 * - strongest / weakest: static consistency-strength target, lowest-RTT routing.
 *
 * Every served response is graded client-side ({@link ClientGrader}) against
 * both returned views with client-observed latency; the verdict feeds the
 * ledger. Writes and linearizable-only targets go to the leader (learned from
 * notLeader redirects).
 */
public final class KvSessionClient implements AutoCloseable {

    private final int applicationId;
    // The client site this session lives at: picks the transport's per-site
    // connection pools (and thus the site's simulated geo distances) and
    // labels this session's ledger rows.
    private final int siteId;
    private final String siteName;
    private final int numServers;
    private final int majority;
    private final ClientMode mode;
    private final int retryLimit;
    private final long lostTimeoutMs;
    private final boolean followerLinReads;
    // Applied by lowest-RTT routing here and by the Pileus selector's own
    // exploration: fraction of reads routed to a uniformly random node.
    private final double explorationFraction;
    // Admission-aware routing (null when disabled): decayed per-node
    // admit/reject ratios, one tracker per registered SLA (read and write
    // separately) so admission pressure is per (node, SLA). Pileus multiplies
    // expected profit by pAdmit(node), lowest-RTT routing minimizes
    // rtt/pAdmit. When enabled it replaces the selector's rejection penalty
    // samples.
    private final Map<Integer, AdmitRates> readAdmitRates;
    private final Map<Integer, AdmitRates> writeAdmitRates;
    // Per-second decay shared by the admit/reject counters and the selector's
    // delivery-rate feedback (both are evidence with the same freshness needs).
    private final double admitRateGamma;

    // Full SLA tables (the application's own registration) and their floors.
    private final Map<Integer, List<RungScorer.Rung>> readSlas;
    private final Map<Integer, List<RungScorer.Rung>> writeSlas;
    private final Map<Integer, Integer> readFloors;
    private final Map<Integer, Integer> writeFloors;

    private final KvTransport transport;
    private final boolean ownsTransport;
    private final RttEstimator rttEstimator;
    private final PileusSelector selector; // null unless the mode routes with Pileus
    // Seeded per session; used for routing exploration and tie-breaks.
    // java.util.Random is thread-safe, so retries dispatched from response
    // threads share it with the injector thread without corruption.
    private final Random random;

    private final AtomicLong requestIdGen = new AtomicLong();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private volatile int leaderHint = -1;

    // The session's causal frontier: the highest log index of anything this
    // session has written or read (-1 = none). Every request carries it and
    // the causal levels wait for the serving view to cover it. It advances on
    // the final delivered response of a write (the entry's index, wc:1
    // included) or a read (the index of the value the grader credited);
    // rejected, lost, and absent-key results do not move it. Grading always
    // uses the value a request was sent with, never the live one, so a read's
    // causal obligation is fixed at the moment it is issued.
    private final AtomicLong sessionIndex = new AtomicLong(-1);

    private final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService lostSweeper;

    private static final class Pending {
        final KvRequest request;
        final long firstSendNanos;
        final long firstSendMs;
        final int targetNode;
        final int attempt;
        final long deadlineNanos;
        // Client-side profit prediction and the target rung's threshold for
        // recomputing the wait bound on retries. targetStrength is the
        // targeted read level (-1 = untargeted) for the selector's delivery
        // feedback; suppressServerWait caps the server-side wait at 1 ms when
        // the selector already scored the target infeasible, so a doomed wait
        // does not burn occupancy.
        final double predictedProfit;
        final double targetThresholdMs;
        final int targetStrength;
        final boolean suppressServerWait;
        volatile KvTransport.RequestHandle transportHandle;

        Pending(KvRequest request, long firstSendNanos, long firstSendMs, int targetNode, int attempt,
                long deadlineNanos,
                double predictedProfit, double targetThresholdMs,
                int targetStrength, boolean suppressServerWait) {
            this.request = request;
            this.firstSendNanos = firstSendNanos;
            this.firstSendMs = firstSendMs;
            this.targetNode = targetNode;
            this.attempt = attempt;
            this.deadlineNanos = deadlineNanos;
            this.predictedProfit = predictedProfit;
            this.targetThresholdMs = targetThresholdMs;
            this.targetStrength = targetStrength;
            this.suppressServerWait = suppressServerWait;
        }
    }

    public KvSessionClient(int applicationId, List<String> hosts, int basePort, ClientMode mode,
            int rttWindowSize, int retryLimit, long lostTimeoutMs,
            Map<Integer, List<RungScorer.Rung>> readSlas, Map<Integer, List<RungScorer.Rung>> writeSlas,
            double explorationFraction, boolean followerLinReads,
            boolean admissionAwareRouting, double admitRateGamma) {
        this(applicationId, 0, "-", new KvFramedTransport(hosts, basePort, 1), true, mode,
                rttWindowSize, retryLimit, lostTimeoutMs, readSlas, writeSlas,
                explorationFraction, followerLinReads, admissionAwareRouting, admitRateGamma, 42L);
    }

    public KvSessionClient(int applicationId, int siteId, String siteName, KvTransport transport,
            ClientMode mode,
            int rttWindowSize, int retryLimit, long lostTimeoutMs,
            Map<Integer, List<RungScorer.Rung>> readSlas, Map<Integer, List<RungScorer.Rung>> writeSlas,
            double explorationFraction, boolean followerLinReads,
            boolean admissionAwareRouting, double admitRateGamma, long seed) {
        this(applicationId, siteId, siteName, transport, false, mode, rttWindowSize, retryLimit, lostTimeoutMs,
                readSlas, writeSlas, explorationFraction, followerLinReads,
                admissionAwareRouting, admitRateGamma, seed);
    }

    private KvSessionClient(int applicationId, int siteId, String siteName, KvTransport transport,
            boolean ownsTransport, ClientMode mode,
            int rttWindowSize, int retryLimit, long lostTimeoutMs,
            Map<Integer, List<RungScorer.Rung>> readSlas, Map<Integer, List<RungScorer.Rung>> writeSlas,
            double explorationFraction, boolean followerLinReads,
            boolean admissionAwareRouting, double admitRateGamma, long seed) {
        this.applicationId = applicationId;
        if (siteId < 0 || siteId >= transport.numSites()) {
            throw new IllegalArgumentException("siteId " + siteId + " is out of range for a transport with "
                    + transport.numSites() + " sites");
        }
        this.siteId = siteId;
        this.siteName = siteName;
        this.transport = transport;
        this.ownsTransport = ownsTransport;
        this.numServers = transport.numServers();
        this.majority = (numServers / 2) + 1;
        this.mode = mode;
        this.retryLimit = retryLimit;
        this.lostTimeoutMs = lostTimeoutMs;
        this.followerLinReads = followerLinReads;
        this.readSlas = Map.copyOf(readSlas);
        this.writeSlas = Map.copyOf(writeSlas);
        this.readFloors = floorsOf(this.readSlas);
        this.writeFloors = floorsOf(this.writeSlas);
        this.explorationFraction = explorationFraction;
        this.readAdmitRates = admissionAwareRouting ? admitRatesPerSla(this.readSlas, admitRateGamma) : null;
        this.writeAdmitRates = admissionAwareRouting ? admitRatesPerSla(this.writeSlas, admitRateGamma) : null;
        this.admitRateGamma = admitRateGamma;
        this.rttEstimator = new RttEstimator(numServers, rttWindowSize);
        // Routing and selector draw from independent deterministic streams
        // (~seed differs from seed under Random's internal scrambling).
        this.random = new Random(seed);
        this.selector = mode.pileusRouting()
                ? new PileusSelector(numServers, majority, rttWindowSize, followerLinReads,
                        explorationFraction, new Random(~seed))
                : null;

        this.lostSweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kv-client-lost-sweeper-app" + applicationId);
            t.setDaemon(true);
            return t;
        });
        this.lostSweeper.scheduleAtFixedRate(this::sweepLost, 1, 1, TimeUnit.SECONDS);
    }

    private static Map<Integer, Integer> floorsOf(Map<Integer, List<RungScorer.Rung>> slas) {
        Map<Integer, Integer> floors = new ConcurrentHashMap<>();
        slas.forEach((slaId, rungs) -> floors.put(slaId,
                rungs.stream().mapToInt(RungScorer.Rung::strength).min().orElseThrow()));
        return floors;
    }

    /** One admission tracker per registered SLA (keyed by slaId). */
    private AdmitRates admitRatesFor(boolean isRead, int slaId) {
        Map<Integer, AdmitRates> rates = isRead ? readAdmitRates : writeAdmitRates;
        return rates == null ? null : rates.get(slaId);
    }

    private Map<Integer, AdmitRates> admitRatesPerSla(Map<Integer, List<RungScorer.Rung>> slas, double gamma) {
        Map<Integer, AdmitRates> rates = new ConcurrentHashMap<>();
        for (Integer slaId : slas.keySet()) {
            rates.put(slaId, new AdmitRates(numServers, gamma));
        }
        return rates;
    }

    private List<RungScorer.Rung> slaOf(Map<Integer, List<RungScorer.Rung>> slas, int slaId) {
        List<RungScorer.Rung> rungs = slas.get(slaId);
        if (rungs == null) {
            throw new IllegalStateException("No SLA registered on the client for applicationId="
                    + applicationId + " slaId=" + slaId);
        }
        return rungs;
    }

    // ===== Sending =====

    public void sendRead(String key, int slaId) {
        long sessionIndex = this.sessionIndex.get();

        List<RungScorer.Rung> sla = slaOf(readSlas, slaId);
        AdmitRates admitRates = admitRatesFor(true, slaId);
        int targetNode;
        RungScorer.Rung targetRung = null;
        double predicted = 0;
        boolean targetFeasible = true;
        switch (mode) {
            case CHAMELEON -> targetNode = lowestRttNode(admitRates);
            case CHAMELEON_PILEUS -> {
                PileusSelector.Choice choice = selector.chooseRead(sla, sessionIndex, leaderHint,
                        admitRates);
                targetNode = choice == null ? lowestRttNode(admitRates) : choice.node();
            }
            case PILEUS -> {
                PileusSelector.Choice choice = selector.chooseRead(sla, sessionIndex, leaderHint,
                        admitRates);
                if (choice == null) {
                    targetNode = lowestRttNode(admitRates);
                } else {
                    targetNode = choice.node();
                    targetRung = sla.get(choice.rungIndex());
                    predicted = choice.expectedProfit();
                    targetFeasible = choice.feasible();
                }
            }
            default -> { // STRONGEST / WEAKEST
                targetRung = sla.get(staticTargetIndex(sla, mode == ClientMode.STRONGEST));
                predicted = targetRung.profit();
                boolean linOnLeaderOnly = targetRung.strength() == ReadLevel.LINEARIZABLE.getNumber()
                        && !followerLinReads;
                targetNode = (linOnLeaderOnly && leaderHint >= 0) ? leaderHint : lowestRttNode(admitRates);
            }
        }

        KvRequest.Builder request = KvRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setApplicationId(applicationId)
                .setSlaId(slaId)
                .setIsRead(true)
                .setKey(key)
                .setSessionIndex(sessionIndex);
        if (targetRung != null) {
            request.setWantLinearizable(targetRung.strength() == ReadLevel.LINEARIZABLE.getNumber());
            request.setTargetReadLevel(ReadLevel.forNumber(targetRung.strength()));
        }
        dispatch(request.build(), targetNode, 1, predicted,
                targetRung == null ? 0 : targetRung.thresholdMs(),
                targetRung == null ? -1 : targetRung.strength(),
                !targetFeasible);
    }

    public void sendWrite(String key, String value, int slaId) {
        List<RungScorer.Rung> sla = slaOf(writeSlas, slaId);
        int targetNode = pickWriteTarget();
        RungScorer.Rung targetRung = null;
        double predicted = 0;
        switch (mode) {
            case CHAMELEON, CHAMELEON_PILEUS -> {
                // Server decides the concern.
            }
            case PILEUS -> {
                PileusSelector.Choice choice = selector.chooseWrite(sla, targetNode);
                targetRung = sla.get(choice.rungIndex());
                predicted = choice.expectedProfit();
            }
            default -> { // STRONGEST / WEAKEST
                targetRung = sla.get(staticTargetIndex(sla, mode == ClientMode.STRONGEST));
                predicted = targetRung.profit();
            }
        }

        long sessionIndex = this.sessionIndex.get();
        KvRequest.Builder request = KvRequest.newBuilder()
                .setRequestId(requestIdGen.incrementAndGet())
                .setApplicationId(applicationId)
                .setSlaId(slaId)
                .setIsRead(false)
                .setKey(key)
                .setValue(value)
                .setSessionIndex(sessionIndex);
        if (targetRung != null) {
            request.setRequestedWriteConcern(Math.min(Math.max(1, targetRung.strength()), majority));
        }
        dispatch(request.build(), targetNode, 1, predicted,
                targetRung == null ? 0 : targetRung.thresholdMs(), -1, false);
    }

    /**
     * The strongest or weakest consistency rung: reads compare ladder levels
     * (EVENTUAL_LOCAL < EVENTUAL_MAJORITY < CAUSAL_LOCAL < CAUSAL_MAJORITY
     * < LINEARIZABLE), writes compare concerns (more acks = stronger).
     * Profit breaks ties within a level: strongest keeps the best-paying
     * rung of the top level, weakest the floor's cheapest.
     */
    private static int staticTargetIndex(List<RungScorer.Rung> sla, boolean strongest) {
        int best = 0;
        for (int i = 1; i < sla.size(); i++) {
            RungScorer.Rung rung = sla.get(i);
            RungScorer.Rung current = sla.get(best);
            boolean betterStrength = strongest
                    ? rung.strength() > current.strength()
                    : rung.strength() < current.strength();
            boolean tieBetterProfit = rung.strength() == current.strength()
                    && (strongest ? rung.profit() > current.profit() : rung.profit() < current.profit());
            if (betterStrength || tieBetterProfit) {
                best = i;
            }
        }
        return best;
    }

    /**
     * Lowest estimated RTT; ties (including the all-cold start) break
     * randomly. A small exploration fraction goes to a uniformly random node
     * instead, so the RTT windows of non-fastest nodes keep getting samples
     * and a recovered node can win the routing back - the same reason the
     * Pileus selector explores.
     *
     * With admission-aware routing the score is rtt / pAdmit instead of raw
     * rtt (admitRates is the requesting SLA's tracker, null when disabled): a
     * node rejecting half its requests looks twice as expensive, so
     * rejections push traffic away without poisoning the RTT estimates. The
     * prior keeps pAdmit above 0, and cold nodes (rtt estimate 0) still score
     * 0 and win their first probes through the random tie-break.
     */
    private int lowestRttNode(AdmitRates admitRates) {
        if (explorationFraction > 0 && random.nextDouble() < explorationFraction) {
            return random.nextInt(numServers);
        }
        double best = Double.MAX_VALUE;
        int count = 0;
        int pick = 0;
        for (int i = 0; i < numServers; i++) {
            double score = rttEstimator.estimateMs(i);
            if (admitRates != null) {
                score /= admitRates.pAdmit(i);
            }
            if (score < best) {
                best = score;
                count = 1;
                pick = i;
            } else if (score == best && random.nextInt(++count) == 0) {
                pick = i;
            }
        }
        return pick;
    }

    private int pickWriteTarget() {
        int leader = leaderHint;
        return leader >= 0 ? leader : Math.floorMod(roundRobin.getAndIncrement(), numServers);
    }

    private void dispatch(KvRequest request, int targetNode, int attempt,
            double predictedProfit, double targetThresholdMs,
            int targetStrength, boolean suppressServerWait) {
        long nowNanos = System.nanoTime();
        long nowMs = System.currentTimeMillis();
        Pending previous = pending.get(request.getRequestId());
        long deadlineNanos = previous != null
                ? previous.deadlineNanos
                : saturatedAdd(nowNanos, TimeUnit.MILLISECONDS.toNanos(
                        (long) Math.ceil(longestDeadlineMs(request))));
        Pending entry = (previous != null)
                // Retry: keep the original send instants so latency stays end
                // to end and the prediction stays the first.
                ? new Pending(request, previous.firstSendNanos, previous.firstSendMs, targetNode, attempt,
                        deadlineNanos,
                        previous.predictedProfit, previous.targetThresholdMs,
                        previous.targetStrength, previous.suppressServerWait)
                : new Pending(request, nowNanos, nowMs, targetNode, attempt,
                        deadlineNanos,
                        predictedProfit, targetThresholdMs,
                        targetStrength, suppressServerWait);
        pending.put(request.getRequestId(), entry);

        // The RTT estimate rides every request (the scorer's rho); in the
        // client-decided modes the wait bound is the d_max analog, the target
        // rung's threshold minus the network's share of it.
        double rtt = rttEstimator.estimateMs(targetNode);
        KvRequest.Builder withRtt = request.toBuilder().setRttEstimateMs(rtt);
        if (entry.suppressServerWait) {
            // The selector scored this target's consistency infeasible: the
            // wait cannot succeed, so do not buy it (1 ms is the smallest
            // bound the wire encodes; <= 0 would mean the maxWaitMs default).
            withRtt.setWaitBoundMs(1.0);
        } else if (entry.targetThresholdMs > 0) {
            withRtt.setWaitBoundMs(Math.max(1.0, entry.targetThresholdMs - rtt));
        }
        long remainingDeadlineNanos = deadlineNanos - System.nanoTime();
        if (remainingDeadlineNanos <= 0) {
            if (pending.remove(request.getRequestId(), entry)) {
                ClientMetricsTracker.recordDeadlineExceeded(targetNode, siteName, chosenLabel(request));
            }
            return;
        }
        long invocationStartedNanos = System.nanoTime();
        entry.transportHandle = transport.execute(siteId, targetNode, withRtt.build(), remainingDeadlineNanos,
                response -> handleResponse(targetNode, response, entry),
                failure -> handleRpcError(targetNode, entry, failure));
        double invocationMs = (System.nanoTime() - invocationStartedNanos) / 1_000_000.0;
        ClientMetricsTracker.recordTransportCall(targetNode, siteName, chosenLabel(request), invocationMs);
    }

    private double longestDeadlineMs(KvRequest request) {
        List<RungScorer.Rung> sla = slaOf(request.getIsRead() ? readSlas : writeSlas, request.getSlaId());
        return sla.stream().mapToDouble(RungScorer.Rung::thresholdMs).max()
                .orElseThrow(() -> new IllegalStateException("SLA has no rungs"));
    }

    private static long saturatedAdd(long left, long right) {
        return right > 0 && left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    // ===== Responses =====

    private void handleResponse(int nodeId, KvResponse response, Pending expected) {
        long responseReceiveNanos = System.nanoTime();
        long responseReceiveEpochMs = System.currentTimeMillis();
        Pending entry = pending.get(response.getRequestId());
        if (entry != expected) {
            return; // late duplicate after a retry resolved, or post-loss reply
        }
        KvRequest request = entry.request;
        String chosen = chosenLabel(request);

        if (response.getDeadlineExceeded()) {
            if (pending.remove(response.getRequestId(), entry)) {
                ClientMetricsTracker.recordDeadlineExceeded(nodeId, siteName, chosen);
            }
            return;
        }

        if (response.getNotLeader()) {
            int hinted = response.getLeaderId();
            if (hinted >= 0 && hinted < numServers) {
                leaderHint = hinted;
            }
            ClientMetricsTracker.recordRedirect(nodeId, siteName, chosen);
            if (entry.attempt < retryLimit) {
                int target = (hinted >= 0 && hinted < numServers)
                        ? hinted
                        : Math.floorMod(roundRobin.getAndIncrement(), numServers);
                dispatch(request, target, entry.attempt + 1, 0, 0, -1, false);
            } else {
                pending.remove(response.getRequestId(), entry);
                ClientMetricsTracker.recordFailure(nodeId, siteName, chosen);
            }
            return;
        }

        if (response.getRejected()) {
            // Admission shed this request; retrying would defeat the shedding,
            // but routing must see the rejection or it would keep targeting
            // the rejecting node on cold-start optimism. With admission-aware
            // routing the decayed admit/reject ratio carries that signal;
            // otherwise the selector's latency-penalty samples do.
            pending.remove(response.getRequestId(), entry);
            // A rejection is an RTT sample like any other reply: its service
            // time covers the scoring and the subtraction leaves the network
            // legs. Feeding it keeps the estimator live under total rejection - otherwise
            // windows poisoned by an overload burst freeze (nothing is served,
            // so nothing updates rho) and the scorer keeps rejecting on the
            // stale estimate long after the queues have drained.
            double rejectLatencyMs = (responseReceiveNanos - entry.firstSendNanos) / 1_000_000.0;
            rttEstimator.observe(nodeId, rejectLatencyMs, response.getServiceTimeMs());
            AdmitRates admitRates = admitRatesFor(request.getIsRead(), request.getSlaId());
            if (admitRates != null) {
                admitRates.onReject(nodeId);
            } else if (selector != null) {
                if (request.getIsRead()) {
                    selector.observeReadRejected(nodeId, request.getWantLinearizable());
                } else {
                    selector.observeWriteRejected(request.getRequestedWriteConcern());
                }
            }
            ClientMetricsTracker.recordRejected(nodeId, siteName, chosen, rejectLatencyMs,
                    response.getServerReplyEpochMs(), responseReceiveEpochMs);
            return;
        }

        if (!response.getOk()) {
            if (entry.attempt < retryLimit) {
                dispatch(request, Math.floorMod(roundRobin.getAndIncrement(), numServers), entry.attempt + 1,
                        0, 0, -1, false);
            } else {
                pending.remove(response.getRequestId(), entry);
                ClientMetricsTracker.recordFailure(nodeId, siteName, chosen);
            }
            return;
        }

        if (!pending.remove(response.getRequestId(), entry)) {
            return;
        }
        double latencyMs = (System.nanoTime() - entry.firstSendNanos) / 1_000_000.0;
        rttEstimator.observe(nodeId, latencyMs, response.getServiceTimeMs());
        AdmitRates admitRates = admitRatesFor(request.getIsRead(), request.getSlaId());
        if (admitRates != null) {
            admitRates.onAdmit(nodeId);
        }
        if (selector != null) {
            selector.observeIndices(nodeId, response.getLogIndex(), response.getCommitIndex());
        }

        ClientGrader.Verdict verdict;
        boolean upgraded;
        String executed;
        if (request.getIsRead()) {
            executed = "R:" + response.getDeliveredReadLevel().name();
            if (selector != null) {
                selector.observeRead(nodeId,
                        request.getWantLinearizable()
                                || response.getDeliveredReadLevel() == ReadLevel.LINEARIZABLE,
                        latencyMs);
            }
            // Grade against the frontier this request was sent with (its
            // causal obligation was fixed at issue time), then let the value
            // it delivered become a cause of everything the session does next.
            verdict = ClientGrader.gradeRead(slaOf(readSlas, request.getSlaId()), response,
                    request.getSessionIndex(), latencyMs);
            advanceSessionIndex(verdict.creditedValueIndex());
            upgraded = verdict.gradedStrength() > readFloors.get(request.getSlaId());
            if (selector != null && entry.targetStrength >= 0) {
                // Close the served-below-target loop: the latency window
                // cannot see a wait that expired into a weaker level.
                selector.observeReadDelivery(nodeId, entry.targetStrength,
                        verdict.gradedStrength() >= entry.targetStrength);
            }
        } else {
            long index = response.getValueIndex();
            executed = "W:" + response.getDeliveredWriteConcern();
            if (selector != null) {
                selector.observeWrite(request.getRequestedWriteConcern() > 0
                        ? request.getRequestedWriteConcern()
                        : response.getDeliveredWriteConcern(), latencyMs);
            }
            verdict = ClientGrader.gradeWrite(slaOf(writeSlas, request.getSlaId()), response, latencyMs);
            upgraded = response.getDeliveredWriteConcern() > writeFloors.get(request.getSlaId());
            advanceSessionIndex(index);
        }

        double predicted = mode.chameleonDecision() ? response.getPredictedProfit() : entry.predictedProfit;
        ClientMetricsTracker.recordResponse(nodeId, siteName, chosen, executed, latencyMs,
                response.getTimedOutAndFellBack(), verdict.deadlineViolation(),
                predicted, verdict.realizedProfit(), verdict.satisfiedRung(), upgraded, response.getWaited());
    }

    /** The session's current causal frontier (-1 = nothing observed yet). */
    public long sessionIndex() {
        return sessionIndex.get();
    }

    private void advanceSessionIndex(long observedIndex) {
        if (observedIndex >= 0) {
            sessionIndex.accumulateAndGet(observedIndex, Math::max);
        }
    }

    private void handleRpcError(int nodeId, Pending entry, KvTransport.RpcFailure failure) {
        Status status = failure.status();
        long requestId = entry.request.getRequestId();
        if (pending.get(requestId) != entry) {
            return;
        }
        if (status.getCode() == Status.Code.DEADLINE_EXCEEDED) {
            if (pending.remove(requestId, entry)) {
                ClientMetricsTracker.recordDeadlineExceeded(nodeId, siteName, chosenLabel(entry.request));
            }
            return;
        }
        if (status.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
            if (!pending.remove(requestId, entry)) {
                return;
            }
            KvRequest request = entry.request;
            double rejectLatencyMs = (System.nanoTime() - entry.firstSendNanos) / 1_000_000.0;
            rttEstimator.observe(nodeId, rejectLatencyMs, 0);
            AdmitRates admitRates = admitRatesFor(request.getIsRead(), request.getSlaId());
            if (admitRates != null) {
                admitRates.onReject(nodeId);
            } else if (selector != null) {
                if (request.getIsRead()) {
                    selector.observeReadRejected(nodeId, request.getWantLinearizable());
                } else {
                    selector.observeWriteRejected(request.getRequestedWriteConcern());
                }
            }
            ClientMetricsTracker.recordRejected(nodeId, siteName, chosenLabel(request), rejectLatencyMs,
                    failure.serverReplyEpochMs(), System.currentTimeMillis());
            return;
        }
        ClientMetricsTracker.recordTransportError(status.getDescription());
        if (entry.attempt < retryLimit && System.nanoTime() < entry.deadlineNanos) {
            dispatch(entry.request, Math.floorMod(roundRobin.getAndIncrement(), numServers),
                    entry.attempt + 1, 0, 0, -1, false);
            return;
        }
        if (pending.remove(requestId, entry)) {
            if (System.nanoTime() >= entry.deadlineNanos) {
                ClientMetricsTracker.recordDeadlineExceeded(nodeId, siteName, chosenLabel(entry.request));
            } else {
                ClientMetricsTracker.recordFailure(nodeId, siteName, chosenLabel(entry.request));
            }
        }
    }

    private static String chosenLabel(KvRequest request) {
        return (request.getIsRead() ? "R:" : "W:") + "A" + request.getApplicationId() + "S" + request.getSlaId();
    }

    private void sweepLost() {
        if (readAdmitRates != null) {
            readAdmitRates.values().forEach(AdmitRates::decay);
        }
        if (writeAdmitRates != null) {
            writeAdmitRates.values().forEach(AdmitRates::decay);
        }
        if (selector != null) {
            // Same forgetting horizon as the admission evidence: a condemned
            // (server, level) drifts back to neutral and can win again.
            selector.decayDeliveryRates(admitRateGamma);
        }
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<Long, Pending>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Pending> e = it.next();
            if (now - e.getValue().firstSendMs >= lostTimeoutMs) {
                it.remove();
                KvTransport.RequestHandle handle = e.getValue().transportHandle;
                if (handle != null) {
                    handle.cancel();
                }
                ClientMetricsTracker.recordLost(e.getValue().targetNode, siteName, chosenLabel(e.getValue().request));
            }
        }
    }

    public int pendingCount() {
        return pending.size();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        lostSweeper.shutdownNow();
        pending.values().forEach(entry -> {
            KvTransport.RequestHandle handle = entry.transportHandle;
            if (handle != null) {
                handle.cancel();
            }
        });
        pending.clear();
        if (ownsTransport) {
            transport.close();
        }
    }
}
