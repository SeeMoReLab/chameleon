package org.example.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.example.Utility.Grading;
import org.example.Utility.RungScorer;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client-facing execution path behind the framed transport admission
 * gate. The ingress event loop opens occupancy before protobuf parsing and
 * closes it at the terminal reply. The rung scorer
 * considers the legal levels for this node and
 * operation are scored against the registered SLA (steps 2-4), the best
 * scoring level wins, and requests worth less than the capacity they consume
 * are rejected (step 5). There are no modes anywhere: degradation under load
 * comes from the shadow price lambda rising, not from anything flipping.
 *
 * The framed transport applies the hard occupancy cap before protobuf
 * parsing, then places accepted work into a bounded service-wide queue. SLA
 * scoring and operation setup
 * run on that queue's workers, allowing inbound callbacks to keep draining.
 * The leader's replication rate bucket remains the final write backstop.
 *
 * Execution (step 6) is unchanged mechanics: eventual levels read a view,
 * causal levels wait for an index, linearizable runs ReadIndex, write
 * concerns wait for replication or commit. The wait is bounded by the loosest
 * surviving server-side threshold of the chosen level (d_max); on expiry the
 * request falls back to the strongest no-wait level of the same view, files
 * its sample under the chosen level at the bound, and files nothing under the
 * fallback (step 8).
 */
public final class KvClientService implements KvRequestHandler, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvClientService.class);

    // Global backstop on any server-side wait (from config).
    private static volatile long MAX_WAIT_MS = 400;
    private static volatile double REPLICATION_BUDGET_PER_SECOND = 20000;
    // Stage 6: followers may serve linearizable reads by fetching a confirmed
    // read index from the leader and waiting for their own commit index to
    // reach it. Off by default until measured.
    private static volatile boolean FOLLOWER_LINEARIZABLE_READS = false;

    // True in the chameleon* modes: the server scorer resolves the subSLA
    // target. False: the dumb path serves the client's explicit target.
    private static volatile boolean CHAMELEON_DECISION = true;

    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        MAX_WAIT_MS = config.server.maxWaitMs;
        REPLICATION_BUDGET_PER_SECOND = config.server.replicationBudgetPerSecond;
        FOLLOWER_LINEARIZABLE_READS = config.server.followerLinearizableReads;
        CHAMELEON_DECISION = config.chameleonDecision();
    }

    /** Test-only: toggle follower linearizable reads without a full config. */
    static void setFollowerLinearizableReadsForTest(boolean enabled) {
        FOLLOWER_LINEARIZABLE_READS = enabled;
    }

    /** Test-only: toggle the decision mode without a full config. */
    static void setChameleonDecisionForTest(boolean enabled) {
        CHAMELEON_DECISION = enabled;
    }

    private static final int READ_LEVELS = 5;

    private final ServerImpl node;
    private final MeasurementPlane plane;
    private final ReplicationRateBucket replicationBucket;
    private final ServerAdmissionQueue admissionQueue;
    private final int admissionCapacity;

    public KvClientService(ServerImpl node, MeasurementPlane plane) {
        this.node = node;
        this.plane = plane;
        this.replicationBucket = new ReplicationRateBucket(REPLICATION_BUDGET_PER_SECOND);
        this.admissionCapacity = configuredAdmissionCapacity();
        this.admissionQueue = new ServerAdmissionQueue(admissionCapacity,
                Runtime.getRuntime().availableProcessors(), node.nodeId(), plane.drainMetrics());
    }

    private static int configuredAdmissionCapacity() {
        double capacity = Math.ceil(1.5 * MeasurementPlane.sMax());
        if (!Double.isFinite(capacity) || capacity < 1 || capacity > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("1.5 * sMax must be in [1, " + Integer.MAX_VALUE
                    + "], got " + capacity);
        }
        return (int) capacity;
    }

    int admissionCapacity() {
        return admissionCapacity;
    }

    @Override
    public void execute(KvRequest request, long tRecvNanos, KvResponseSink out) {
        if (out.isFinished()) {
            return;
        }
        if (node.isDropAllServerNetworkTraffic()) {
            send(out, failure(request), tRecvNanos);
            return;
        }
        if (out.remainingDeadlineMs() <= 0) {
            send(out, deadlineExceeded(request), tRecvNanos);
            return;
        }
        try {
            if (!admissionQueue.trySubmit(() -> processQueued(request, tRecvNanos, out))) {
                plane.drainMetrics().rejected(ServerDrainMetrics.RejectionReason.ADMISSION_QUEUE_FULL);
                send(out, rejected(request), tRecvNanos);
            }
        } catch (RejectedExecutionException e) {
            LOG.error("Admission executor rejected request {} on server {}",
                    request.getRequestId(), node.nodeId(), e);
            send(out, failure(request), tRecvNanos);
        }
    }

    private void processQueued(KvRequest request, long tRecvNanos, KvResponseSink responseObserver) {
        if (responseObserver.isFinished()) {
            return;
        }
        if (responseObserver.remainingDeadlineMs() <= 0) {
            send(responseObserver, deadlineExceeded(request), tRecvNanos);
            return;
        }
        try {
            handle(request, tRecvNanos, responseObserver);
        } catch (Exception e) {
            LOG.error("Request {} failed on server {}", request.getRequestId(), node.nodeId(), e);
            send(responseObserver, failure(request), tRecvNanos);
        }
    }

    private void handle(KvRequest request, long tRecvNanos, KvResponseSink out) {
        if (request.getIsRead()) {
            if (CHAMELEON_DECISION) {
                handleRead(request, tRecvNanos, out);
            } else {
                plane.drainMetrics().executionAdmitted();
                dumbRead(request, tRecvNanos, out);
            }
        } else {
            if (CHAMELEON_DECISION) {
                handleWrite(request, tRecvNanos, out);
            } else {
                dumbWrite(request, tRecvNanos, out);
            }
        }
    }

    // ===== Dumb path (pileus / strongest / weakest modes) =====
    //
    // The client resolved the subSLA target; the server serves what it has.
    // Reads return both views immediately; the only read wait is the
    // ReadIndex round when linearizability was explicitly requested, and
    // write waits are pure ack semantics for the requested concern. Both are
    // clamped by min(client wait bound, maxWaitMs). No scoring, no price, no
    // upgrades; admission is the occupancy cap (in handle) plus the
    // replication bucket.

    /** The client's d_max analog: its wait bound, clamped to maxWaitMs. */
    private long requestBound(KvRequest request, KvResponseSink out) {
        double bound = request.getWaitBoundMs();
        long configured = bound > 0 ? Math.min((long) Math.ceil(bound), MAX_WAIT_MS) : MAX_WAIT_MS;
        return boundByRpcDeadline(out, configured);
    }

    /** Confirm linearizability and make the committed view cover this session's causal frontier. */
    private CompletableFuture<Void> linearizableReady(KvRequest request) {
        CompletableFuture<Integer> confirmed = node.isLeader()
                ? node.confirmLeadership()
                : node.readIndexFromLeader();
        return confirmed.thenCompose(readIndex -> {
            int requiredIndex = Math.max(readIndex, request.getSessionIndex());
            return node.currentCommitIndex() >= requiredIndex
                    ? CompletableFuture.completedFuture(null)
                    : node.awaitCommitIndex(requiredIndex);
        });
    }

    private void dumbRead(KvRequest request, long tRecvNanos, KvResponseSink out) {
        if (!request.getWantLinearizable()) {
            send(out, dumbReadReply(request, ReadLevel.EVENTUAL_MAJORITY, false, false), tRecvNanos);
            return;
        }
        if (!node.isLeader() && !FOLLOWER_LINEARIZABLE_READS) {
            send(out, notLeader(request), tRecvNanos);
            return;
        }
        CompletableFuture<Void> ready = linearizableReady(request);
        ready.orTimeout(requestBound(request, out), TimeUnit.MILLISECONDS)
                .whenCompleteAsync((v, error) -> {
                    if (error == null) {
                        send(out, dumbReadReply(request, ReadLevel.LINEARIZABLE, true, false), tRecvNanos);
                    } else if (unwrap(error) instanceof ServerImpl.NotLeaderException) {
                        send(out, notLeader(request), tRecvNanos);
                    } else {
                        send(out, dumbReadReply(request, ReadLevel.EVENTUAL_MAJORITY, true, true), tRecvNanos);
                    }
                }, node.executorService);
    }

    private KvResponse.Builder dumbReadReply(KvRequest request, ReadLevel delivered, boolean waited,
            boolean fellBack) {
        KvStore.Versioned committed = node.kv.readCommitted(request.getKey());
        return withViews(KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setValue(committed == null ? "" : committed.value())
                .setValueIndex(committed == null ? -1 : committed.index())
                .setDeliveredReadLevel(delivered)
                .setWaited(waited)
                .setTimedOutAndFellBack(fellBack)
                .setSatisfiedRung(-1), request.getKey());
    }

    private void dumbWrite(KvRequest request, long tRecvNanos, KvResponseSink out) {
        if (!node.isLeader()) {
            send(out, notLeader(request), tRecvNanos);
            return;
        }
        int writeConcern = Math.min(Math.max(1, request.getRequestedWriteConcern()), node.majority());
        if (!replicationBucket.tryCharge()) {
            plane.drainMetrics().rejected(ServerDrainMetrics.RejectionReason.REPLICATION_BUDGET);
            send(out, rejected(request), tRecvNanos);
            return;
        }
        plane.drainMetrics().executionAdmitted();
        int index;
        try {
            index = node.appendWrite(request.getKey(), request.getValue(),
                    request.getApplicationId() + "-" + request.getRequestId());
        } catch (ServerImpl.NotLeaderException e) {
            send(out, notLeader(request).setLeaderId(e.leaderId), tRecvNanos);
            return;
        }
        if (writeConcern <= 1) {
            send(out, dumbWriteReply(request, index, 1, false, false), tRecvNanos);
            return;
        }
        CompletableFuture<Void> wait = (writeConcern >= node.majority())
                ? node.awaitCommitIndex(index)
                : node.awaitReplication(index, writeConcern);
        awaitBounded(wait, requestBound(request, out),
                () -> send(out, dumbWriteReply(request, index, writeConcern, true, false), tRecvNanos),
                () -> send(out, dumbWriteReply(request, index, node.replicaCount(index), true, true), tRecvNanos));
    }

    private KvResponse.Builder dumbWriteReply(KvRequest request, int entryIndex, int deliveredWriteConcern,
            boolean waited, boolean fellBack) {
        return stamp(KvResponse.newBuilder())
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setValueIndex(entryIndex)
                .setDeliveredWriteConcern(deliveredWriteConcern)
                .setWaited(waited)
                .setTimedOutAndFellBack(fellBack)
                .setSatisfiedRung(-1);
    }

    /**
     * Both views, populated on every read reply in every mode. The node's
     * indices are stamped BEFORE the views are read: the client grades causal
     * claims from these indices against the views' value indices, so the
     * published frontier must never exceed the state the views were read from
     * (state-before-index, mirrored onto the reply).
     */
    private KvResponse.Builder withViews(KvResponse.Builder reply, String key) {
        stamp(reply);
        KvStore.Versioned local = node.kv.readLocal(key);
        KvStore.Versioned committed = node.kv.readCommitted(key);
        return reply
                .setLocalValue(local == null ? "" : local.value())
                .setLocalValueIndex(local == null ? -1 : local.index())
                .setCommittedValue(committed == null ? "" : committed.value())
                .setCommittedValueIndex(committed == null ? -1 : committed.index());
    }

    /** Stamp the node's current indices onto a reply. */
    private KvResponse.Builder stamp(KvResponse.Builder reply) {
        return reply.setLogIndex(node.lastLogIndex()).setCommitIndex(node.currentCommitIndex());
    }

    // ===== Scoring machinery =====

    private static final class Candidate {
        final int levelIndex;      // read-level ordinal, or the write concern for writes
        final int gapBucket;
        final RungScorer.ScoredLevel scored;
        final boolean uncalibrated;
        boolean excluded;

        Candidate(int levelIndex, int gapBucket, RungScorer.ScoredLevel scored, boolean uncalibrated) {
            this.levelIndex = levelIndex;
            this.gapBucket = gapBucket;
            this.scored = scored;
            this.uncalibrated = uncalibrated;
        }
    }

    private Candidate scoreLevel(List<RungScorer.Rung> sla, int histogramLevelIndex, int strength, int gapBucket,
            double rho, double lambda) {
        ServiceTimeHistograms.Snapshot snapshot = plane.histograms().snapshot(histogramLevelIndex, gapBucket);
        RungScorer.ScoredLevel scored = RungScorer.score(sla, strength, rho,
                snapshot::fractionAtMost, snapshot.meanMs, lambda);
        return new Candidate(strength, gapBucket, scored, snapshot.totalCount <= 0);
    }

    /**
     * Step 4g plus the cold-start rider cap: the highest score wins, ties go
     * to the weakest level (an upgrade must be strictly better), and a level
     * whose cell is uncalibrated is only eligible while the rider cap has
     * room. Returns null when everything scoring positive is excluded or
     * nothing scores above zero (step 5 rejection).
     */
    private Candidate choose(List<Candidate> candidates, java.util.function.IntUnaryOperator histogramIndexOf) {
        while (true) {
            Candidate best = null;
            for (Candidate candidate : candidates) {
                if (candidate.excluded) {
                    continue;
                }
                if (best == null || candidate.scored.value() > best.scored.value()) {
                    best = candidate;
                }
            }
            if (best == null || best.scored.value() <= 0) {
                return null;
            }
            if (!best.uncalibrated) {
                return best;
            }
            if (plane.tryAcquireUncalibratedRider(histogramIndexOf.applyAsInt(best.levelIndex), best.gapBucket)) {
                return best;
            }
            best.excluded = true;
        }
    }

    // ===== Reads =====

    private void handleRead(KvRequest request, long tRecvNanos, KvResponseSink out) {
        List<RungScorer.Rung> sla = SlaRegistry.readSla(request.getApplicationId(), request.getSlaId());
        if (sla == null) {
            LOG.error("No read SLA registered for applicationId={} slaId={}", request.getApplicationId(),
                    request.getSlaId());
            send(out, failure(request), tRecvNanos);
            return;
        }
        boolean leader = node.isLeader();
        // Linearizable is leader-only unless follower reads are enabled.
        int maxLevel = (leader || FOLLOWER_LINEARIZABLE_READS) ? READ_LEVELS - 1 : READ_LEVELS - 2;
        double rho = effectiveRttMs(request, out);
        double lambda = plane.lambda();
        int sessionIndex = request.getSessionIndex();

        List<Candidate> candidates = new ArrayList<>(READ_LEVELS);
        int satisfiableAnywhere = 0;
        for (int level = 0; level <= maxLevel; level++) {
            // Step 3: the gap each level must close, bucketed coarsely.
            long gap = switch (ReadLevel.forNumber(level)) {
                case CAUSAL_LOCAL -> (long) sessionIndex - node.lastLogIndex();
                case CAUSAL_MAJORITY, LINEARIZABLE ->
                    (long) sessionIndex - node.currentCommitIndex();
                default -> 0L;
            };
            Candidate candidate = scoreLevel(sla, level, level, ServiceTimeHistograms.gapBucketOf(gap), rho, lambda);
            satisfiableAnywhere += candidate.scored.satisfiableRungs();
            candidates.add(candidate);
        }

        // Step 2: if no rung is satisfiable by any level legal on this node
        // (an SLA that only pays for linearizable, on a follower), the request
        // is illegal here: redirect rather than reject.
        if (satisfiableAnywhere == 0 && !leader) {
            send(out, notLeader(request), tRecvNanos);
            return;
        }

        Candidate chosen = choose(candidates, level -> level);
        if (chosen == null) {
            plane.drainMetrics().rejected(ServerDrainMetrics.RejectionReason.SCORER);
            send(out, rejected(request), tRecvNanos);
            return;
        }

        plane.drainMetrics().executionAdmitted();
        executeRead(request, tRecvNanos, out, chosen);
    }

    private void executeRead(KvRequest request, long tRecvNanos, KvResponseSink out, Candidate chosen) {
        String key = request.getKey();
        ReadLevel level = ReadLevel.forNumber(chosen.levelIndex);
        long boundMs = boundByRpcDeadline(out,
                Math.min((long) Math.ceil(chosen.scored.dMaxMs()), MAX_WAIT_MS));

        switch (level) {
            case EVENTUAL_LOCAL -> completeRead(request, tRecvNanos, out, chosen, ReadLevel.EVENTUAL_LOCAL,
                    node.kv.readLocal(key), false, false, 0);
            case EVENTUAL_MAJORITY -> completeRead(request, tRecvNanos, out, chosen, ReadLevel.EVENTUAL_MAJORITY,
                    node.kv.readCommitted(key), false, false, 0);
            case CAUSAL_LOCAL -> {
                int anchor = request.getSessionIndex();
                if (anchor < 0 || node.lastLogIndex() >= anchor) {
                    completeRead(request, tRecvNanos, out, chosen, ReadLevel.CAUSAL_LOCAL,
                            node.kv.readLocal(key), false, false, 0);
                } else {
                    awaitBounded(node.awaitLocalLogIndex(anchor), boundMs,
                            () -> completeRead(request, tRecvNanos, out, chosen, ReadLevel.CAUSAL_LOCAL,
                                    node.kv.readLocal(key), true, false, 0),
                            () -> completeRead(request, tRecvNanos, out, chosen, ReadLevel.EVENTUAL_LOCAL,
                                    node.kv.readLocal(key), true, true, boundMs));
                }
            }
            case CAUSAL_MAJORITY -> {
                int anchor = request.getSessionIndex();
                if (anchor < 0 || node.currentCommitIndex() >= anchor) {
                    completeRead(request, tRecvNanos, out, chosen, ReadLevel.CAUSAL_MAJORITY,
                            node.kv.readCommitted(key), false, false, 0);
                } else {
                    awaitBounded(node.awaitCommitIndex(anchor), boundMs,
                            () -> completeRead(request, tRecvNanos, out, chosen, ReadLevel.CAUSAL_MAJORITY,
                                    node.kv.readCommitted(key), true, false, 0),
                            () -> completeRead(request, tRecvNanos, out, chosen, ReadLevel.EVENTUAL_MAJORITY,
                                    node.kv.readCommitted(key), true, true, boundMs));
                }
            }
            case LINEARIZABLE -> {
                // Leader: ReadIndex confirmation; the committed view already
                // covers the returned index (state-before-index). Follower
                // (flag-gated): fetch the confirmed index from the leader,
                // then wait for the own commit index to reach it.
                CompletableFuture<Void> ready = linearizableReady(request);
                ready.orTimeout(boundMs, TimeUnit.MILLISECONDS)
                        .whenCompleteAsync((v, error) -> {
                            if (error == null) {
                                completeRead(request, tRecvNanos, out, chosen, ReadLevel.LINEARIZABLE,
                                        node.kv.readCommitted(key), true, false, 0);
                            } else if (unwrap(error) instanceof ServerImpl.NotLeaderException) {
                                releaseRider(chosen);
                                send(out, notLeader(request), tRecvNanos);
                            } else {
                                completeRead(request, tRecvNanos, out, chosen, ReadLevel.EVENTUAL_MAJORITY,
                                        node.kv.readCommitted(key), true, true, boundMs);
                            }
                        }, node.executorService);
            }
            default -> send(out, failure(request), tRecvNanos);
        }
    }

    /**
     * Terminal read path: file the sample (step 8: under the chosen level; at
     * the bound when the wait was abandoned), release any cold-start rider,
     * grade what was actually delivered (step 7), and reply.
     */
    private void completeRead(KvRequest request, long tRecvNanos, KvResponseSink out, Candidate chosen,
            ReadLevel delivered, KvStore.Versioned versioned, boolean waited, boolean fellBack, long abandonedAtMs) {
        double serviceMs = (System.nanoTime() - tRecvNanos) / 1_000_000.0;
        plane.fileServiceTime(chosen.levelIndex, chosen.gapBucket, fellBack ? abandonedAtMs : serviceMs);
        releaseRider(chosen);

        // Step 7: grade against the state that served the value. Realized
        // profit is judged on the client's total time, service plus rho.
        int valueIndex = versioned == null ? -1 : versioned.index();
        int commitIndex = node.currentCommitIndex();
        boolean localView = delivered == ReadLevel.EVENTUAL_LOCAL || delivered == ReadLevel.CAUSAL_LOCAL;
        int graded = Grading.gradeRead(delivered, valueIndex, commitIndex,
                localView ? node.lastLogIndex() : commitIndex,
                request.getSessionIndex());
        Grading.Realized realized = Grading.realize(
                SlaRegistry.readSla(request.getApplicationId(), request.getSlaId()),
                graded, serviceMs + request.getRttEstimateMs());

        send(out, withViews(KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setValue(versioned == null ? "" : versioned.value())
                .setValueIndex(valueIndex)
                .setDeliveredReadLevel(delivered)
                .setWaited(waited)
                .setTimedOutAndFellBack(fellBack)
                .setSatisfiedRung(realized.rungIndex())
                .setRealizedProfit(realized.profit())
                .setPredictedProfit(chosen.scored.expectedProfit())
                .setGradedReadStrength(graded), request.getKey()), tRecvNanos);
    }

    // ===== Writes =====

    private void handleWrite(KvRequest request, long tRecvNanos, KvResponseSink out) {
        if (!node.isLeader()) {
            // Writes are illegal on followers: redirect rather than reject (step 2).
            send(out, notLeader(request), tRecvNanos);
            return;
        }
        List<RungScorer.Rung> sla = SlaRegistry.writeSla(request.getApplicationId(), request.getSlaId());
        if (sla == null) {
            LOG.error("No write SLA registered for applicationId={} slaId={}", request.getApplicationId(),
                    request.getSlaId());
            send(out, failure(request), tRecvNanos);
            return;
        }
        double rho = effectiveRttMs(request, out);
        double lambda = plane.lambda();

        List<Candidate> candidates = new ArrayList<>(node.majority());
        for (int wc = 1; wc <= node.majority(); wc++) {
            candidates.add(scoreLevel(sla, plane.writeLevelIndex(wc), wc, 0, rho, lambda));
        }
        Candidate chosen = choose(candidates, plane::writeLevelIndex);
        if (chosen == null) {
            plane.drainMetrics().rejected(ServerDrainMetrics.RejectionReason.SCORER);
            send(out, rejected(request), tRecvNanos);
            return;
        }
        // The replication resource: one entry per admitted write, charged at
        // admission, never returned. Empty bucket is the hard backstop.
        if (!replicationBucket.tryCharge()) {
            releaseRider(chosen);
            plane.drainMetrics().rejected(ServerDrainMetrics.RejectionReason.REPLICATION_BUDGET);
            send(out, rejected(request), tRecvNanos);
            return;
        }

        plane.drainMetrics().executionAdmitted();
        int writeConcern = chosen.levelIndex;
        int index;
        try {
            index = node.appendWrite(request.getKey(), request.getValue(),
                    request.getApplicationId() + "-" + request.getRequestId());
        } catch (ServerImpl.NotLeaderException e) {
            releaseRider(chosen);
            send(out, notLeader(request).setLeaderId(e.leaderId), tRecvNanos);
            return;
        }

        if (writeConcern <= 1) {
            completeWrite(request, tRecvNanos, out, chosen, index, 1, false, false, 0);
            return;
        }

        long boundMs = boundByRpcDeadline(out,
                Math.min((long) Math.ceil(chosen.scored.dMaxMs()), MAX_WAIT_MS));
        CompletableFuture<Void> wait = (writeConcern >= node.majority())
                ? node.awaitCommitIndex(index)
                : node.awaitReplication(index, writeConcern);
        awaitBounded(wait, boundMs,
                () -> completeWrite(request, tRecvNanos, out, chosen, index, writeConcern, true, false, 0),
                () -> completeWrite(request, tRecvNanos, out, chosen, index, node.replicaCount(index), true, true,
                        boundMs));
    }

    private void completeWrite(KvRequest request, long tRecvNanos, KvResponseSink out, Candidate chosen,
            int entryIndex, int deliveredWriteConcern, boolean waited, boolean fellBack, long abandonedAtMs) {
        double serviceMs = (System.nanoTime() - tRecvNanos) / 1_000_000.0;
        plane.fileServiceTime(plane.writeLevelIndex(chosen.levelIndex), chosen.gapBucket,
                fellBack ? abandonedAtMs : serviceMs);
        if (chosen.uncalibrated) {
            plane.releaseUncalibratedRider(plane.writeLevelIndex(chosen.levelIndex), chosen.gapBucket);
        }

        // Step 7: writes cannot be upgraded after the fact; the graded
        // strength is the replication count at acknowledgment.
        Grading.Realized realized = Grading.realize(
                SlaRegistry.writeSla(request.getApplicationId(), request.getSlaId()),
                deliveredWriteConcern, serviceMs + request.getRttEstimateMs());

        send(out, stamp(KvResponse.newBuilder())
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setValueIndex(entryIndex)
                .setDeliveredWriteConcern(deliveredWriteConcern)
                .setWaited(waited)
                .setTimedOutAndFellBack(fellBack)
                .setSatisfiedRung(realized.rungIndex())
                .setRealizedProfit(realized.profit())
                .setPredictedProfit(chosen.scored.expectedProfit()), tRecvNanos);
    }

    // ===== Helpers =====

    private void releaseRider(Candidate chosen) {
        if (chosen.uncalibrated) {
            plane.releaseUncalibratedRider(chosen.levelIndex, chosen.gapBucket);
        }
    }

    private double effectiveRttMs(KvRequest request, KvResponseSink out) {
        return Math.max(request.getRttEstimateMs(), out.ingressElapsedMs());
    }

    private long boundByRpcDeadline(KvResponseSink out, long requestedMs) {
        return Math.max(1, Math.min(requestedMs, (long) Math.floor(out.remainingDeadlineMs())));
    }

    private void awaitBounded(CompletableFuture<Void> wait, long boundMs, Runnable onSatisfied, Runnable onExpired) {
        wait.orTimeout(boundMs, TimeUnit.MILLISECONDS)
                .whenCompleteAsync((v, error) -> {
                    if (error == null) {
                        onSatisfied.run();
                    } else {
                        onExpired.run();
                    }
                }, node.executorService);
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException
                || t instanceof java.util.concurrent.ExecutionException) && t.getCause() != null ? t.getCause() : t;
    }

    private KvResponse.Builder notLeader(KvRequest request) {
        return stamp(KvResponse.newBuilder())
                .setRequestId(request.getRequestId())
                .setOk(false)
                .setNotLeader(true)
                .setSatisfiedRung(-1)
                .setLeaderId(node.leaderIdHint());
    }

    private KvResponse.Builder rejected(KvRequest request) {
        return stamp(KvResponse.newBuilder())
                .setRequestId(request.getRequestId())
                .setOk(false)
                .setRejected(true)
                .setSatisfiedRung(-1)
                .setLeaderId(-1);
    }

    private KvResponse.Builder failure(KvRequest request) {
        return stamp(KvResponse.newBuilder()).setRequestId(request.getRequestId()).setOk(false)
                .setSatisfiedRung(-1)
                .setLeaderId(-1);
    }

    private KvResponse.Builder deadlineExceeded(KvRequest request) {
        return stamp(KvResponse.newBuilder()).setRequestId(request.getRequestId()).setOk(false)
                .setDeadlineExceeded(true)
                .setSatisfiedRung(-1)
                .setLeaderId(-1);
    }

    private void send(KvResponseSink out, KvResponse.Builder reply, long tRecvNanos) {
        out.respond(reply);
    }

    @Override
    public void close() {
        admissionQueue.close();
    }
}
