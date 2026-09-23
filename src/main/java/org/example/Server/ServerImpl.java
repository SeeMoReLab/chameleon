package org.example.Server;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.example.Timer.CustomTimer;
import org.example.Utility.LogEntry;
import org.example.Utility.RaftLog;
import org.example.Utility.ServerStatus.ServerCurrentStatus;
import org.example.raft.AppendEntriesArgument;
import org.example.raft.AppendEntriesResult;
import org.example.raft.LogEntryProto;
import org.example.raft.RaftGrpc;
import org.example.raft.RaftGrpc.RaftStub;
import org.example.raft.ReadIndexRequest;
import org.example.raft.ReadIndexResult;
import org.example.raft.RequestVoteArguments;
import org.example.raft.RequestVoteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * One Raft node: election, log replication, commit tracking, the KV state
 * machine (local and committed views), ReadIndex leadership confirmation, and
 * the wait registries the per-request path (KvClientService) blocks on. All
 * request-level decisions live in KvClientService; this class only offers the
 * mechanics: appendWrite, awaitLocalLogIndex, awaitCommitIndex,
 * awaitReplication, confirmLeadership.
 */
public class ServerImpl extends RaftGrpc.RaftImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ServerImpl.class);

    final int NUM_OF_SERVERS;
    final int serverId;

    public final AtomicInteger currentTerm;
    private int votedFor;
    final RaftLog log;
    final AtomicInteger commitIndex;
    private final AtomicIntegerArray nextIndex;
    private final AtomicIntegerArray matchIndex;
    private final ReplicationPipeline[] replicationPipelines;
    private final AtomicIntegerArray maxObservedReplicationInflight;
    private final AtomicInteger maxObservedReplicationBatchEntries = new AtomicInteger();

    private final CustomTimer electionTimer;
    public RaftStub[] stubs;
    private final AtomicInteger votes;
    public volatile ServerCurrentStatus status;
    private final AtomicBoolean isElectionOver;
    volatile int currentLeader = -1;

    final ReadWriteLock lock;

    // Replicated KV state machine: local view applied at log append (may roll
    // back), committed view applied at commit-index advance (never rolls back).
    final KvStore kv;

    // Wait registries the per-request path blocks on. Index registries are
    // monotonic: after a truncation the old high-water mark stands, which is
    // correct because a wait anchored on a rolled-back entry has nothing left
    // to wait for.
    private final IndexWaitRegistry localLogWaiters = new IndexWaitRegistry();
    private final IndexWaitRegistry commitWaiters = new IndexWaitRegistry();
    private final ReplicationWaitRegistry replicationWaiters = new ReplicationWaitRegistry();

    // A single event loop owns every leader-side replication pipeline. RPC
    // callbacks enqueue onto it, so speculative cursors and in-flight deques
    // never need cross-thread mutation.
    private final ScheduledExecutorService replicationExecutor;
    private final ScheduledExecutorService waitScheduler;
    final ExecutorService executorService;

    // When true, this node stops all Raft inter-server network communication.
    private volatile boolean dropAllServerNetworkTraffic;

    // Every gRPC channel this node opens, so shutdown() can close them all.
    private final List<ManagedChannel> ownedChannels = Collections.synchronizedList(new ArrayList<>());

    // Liveness only: how often an idle leader must prove itself to a follower.
    // Unrelated to how long entries may accumulate, which is the linger below.
    private static final int HEARTBEAT_INTERVAL_MS = 20;
    private static volatile int MAX_ENTRIES_PER_REPLICATION_BATCH = 4000;
    private static volatile int MAX_INFLIGHT_REPLICATION_BATCHES_PER_FOLLOWER = 4;
    // Smallest gap between two sends to the same follower when the batch would
    // be short of the cap. Without it the pipeline is self-clocked: every
    // follower acknowledgement immediately pumps the next batch, so batch size
    // is set by how many entries happen to have arrived in one round trip, and
    // a low-latency topology therefore batches 40x worse than a high-latency
    // one for the same work (MONGODB_BASELINES.md). A full batch never waits,
    // and the heartbeat pump above still guarantees progress, so this is a
    // floor on batch size rather than a delay on throughput.
    private static volatile long REPLICATION_LINGER_NANOS =
            TimeUnit.MILLISECONDS.toNanos(1);
    // HTTP/2 flow-control window for the server-server path, set on both the
    // replication channels and the Raft server (the receiver grants the
    // window, so the server side is the one that governs leader-to-follower
    // data). Must hold maxInflight x maxEntries batches in serialized bytes,
    // or the byte window - not the batch cap - limits pipelining. Setting it
    // explicitly disables gRPC's BDP auto-tuning, which is intended: the
    // window is then a fixed, reproducible experiment parameter.
    private static volatile int REPLICATION_FLOW_CONTROL_WINDOW_BYTES = 1 << 20; // gRPC's own default
    private static final int ELECTION_TIMEOUT_BASE_MS = 5000;
    private static final int ELECTION_TIMEOUT_JITTER_MS = 1000;

    // Cluster wiring (from config; see ExperimentConfig.cluster)
    private static volatile List<String> SERVER_HOSTS = List.of("localhost", "localhost", "localhost");
    private static volatile int SERVER_BASE_PORT = 8000;
    // First-election cue (-1 = none): the preferred node runs a shorter,
    // jitter-free election timeout, so it reliably times out and wins the
    // first election while every other node still sleeps; its heartbeats then
    // keep their timers suppressed. A cue, not a constraint - Raft safety is
    // untouched and any node can win once the preferred one is down. The
    // preference also reapplies on timer resets, so a recovered preferred
    // node tends to win the leadership back at the next election it sees.
    private static volatile int PREFERRED_LEADER_ID = -1;
    // Base PRNG seed (config `seed`); the only server-side randomness is the
    // election-timeout jitter.
    private static volatile long SEED = 42;

    private record InflightBatch(long sequence, int startIndex, int endExclusive,
            int term, long sendStartNanos) {
        int acknowledgedIndex() {
            return endExclusive - 1;
        }
    }

    /** Mutable only on {@link #replicationExecutor}. */
    private static final class ReplicationPipeline {
        final int followerId;
        final Deque<InflightBatch> inFlight = new ArrayDeque<>();
        long generation;
        long nextSequence;
        int term = -1;
        int nextSendIndex;
        long lastSendNanos;
        boolean connecting;
        ClientCallStreamObserver<AppendEntriesArgument> outbound;

        ReplicationPipeline(int followerId) {
            this.followerId = followerId;
        }

        void resetSpeculativeState(int confirmedNextIndex, int leaderTerm) {
            inFlight.clear();
            nextSendIndex = confirmedNextIndex;
            term = leaderTerm;
            lastSendNanos = 0;
        }
    }

    /** Apply cluster configuration. Must run before any ServerImpl is constructed. */
    public static void applyConfig(org.example.Utility.ExperimentConfig config) {
        applyClusterSettings(config.cluster.serverHosts, config.cluster.serverBasePort);
        PREFERRED_LEADER_ID = config.cluster.preferredLeaderId;
        SEED = config.seed;
        MAX_ENTRIES_PER_REPLICATION_BATCH = config.server.maxEntriesPerReplicationBatch;
        MAX_INFLIGHT_REPLICATION_BATCHES_PER_FOLLOWER =
                config.server.maxInflightReplicationBatchesPerFollower;
        REPLICATION_FLOW_CONTROL_WINDOW_BYTES = config.server.replicationFlowControlWindowBytes;
        REPLICATION_LINGER_NANOS =
                TimeUnit.MICROSECONDS.toNanos(Math.round(config.server.replicationLingerMs * 1000.0));
    }

    /** The Raft server must announce the same window the channels assume. */
    public static int replicationFlowControlWindowBytes() {
        return REPLICATION_FLOW_CONTROL_WINDOW_BYTES;
    }

    static void applyReplicationSettingsForTest(int maxEntriesPerBatch, int maxInflightBatches) {
        applyReplicationSettingsForTest(maxEntriesPerBatch, maxInflightBatches, 0.0);
    }

    static void applyReplicationSettingsForTest(int maxEntriesPerBatch, int maxInflightBatches,
            double lingerMs) {
        if (maxEntriesPerBatch <= 0 || maxInflightBatches <= 0) {
            throw new IllegalArgumentException("replication batch and in-flight limits must be positive");
        }
        if (!(lingerMs >= 0)) {
            throw new IllegalArgumentException("replication linger must be non-negative, got " + lingerMs);
        }
        MAX_ENTRIES_PER_REPLICATION_BATCH = maxEntriesPerBatch;
        MAX_INFLIGHT_REPLICATION_BATCHES_PER_FOLLOWER = maxInflightBatches;
        REPLICATION_LINGER_NANOS = TimeUnit.MICROSECONDS.toNanos(Math.round(lingerMs * 1000.0));
    }

    /** The linger in nanoseconds, for tests and the pump's own scheduling. */
    static long replicationLingerNanos() {
        return REPLICATION_LINGER_NANOS;
    }

    /** Point the peer stubs at a cluster without a full config (tests use this directly). */
    public static void applyClusterSettings(List<String> serverHosts, int basePort) {
        SERVER_HOSTS = List.copyOf(serverHosts);
        SERVER_BASE_PORT = basePort;
    }

    public ServerImpl(int serverId, int NUM_OF_SERVERS) {
        this.serverId = serverId;
        this.NUM_OF_SERVERS = NUM_OF_SERVERS;
        this.dropAllServerNetworkTraffic = false;
        this.currentTerm = new AtomicInteger(0);
        this.votedFor = -1;
        this.commitIndex = new AtomicInteger(-1);
        this.status = ServerCurrentStatus.FOLLOWER;
        this.votes = new AtomicInteger(0);
        this.isElectionOver = new AtomicBoolean(false);
        this.lock = new ReentrantReadWriteLock();
        this.kv = new KvStore();
        this.log = new RaftLog();
        this.stubs = new RaftStub[NUM_OF_SERVERS];
        this.nextIndex = new AtomicIntegerArray(NUM_OF_SERVERS);
        this.matchIndex = new AtomicIntegerArray(NUM_OF_SERVERS);
        this.replicationPipelines = new ReplicationPipeline[NUM_OF_SERVERS];
        this.maxObservedReplicationInflight = new AtomicIntegerArray(NUM_OF_SERVERS);
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            nextIndex.set(i, 0);
            matchIndex.set(i, -1);
            if (i != serverId) {
                replicationPipelines[i] = new ReplicationPipeline(i);
            }
        }
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.replicationExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "raft-replication-" + serverId);
            thread.setDaemon(true);
            return thread;
        });
        // The pump must run at least as often as the linger, or a batch held back
        // by it would wait for the heartbeat tick instead. Empty heartbeats stay
        // rate-limited to HEARTBEAT_INTERVAL_MS inside the send path, so a fast
        // tick costs nothing on an idle leader.
        long pumpPeriodNanos = Math.max(1, Math.min(
                TimeUnit.MILLISECONDS.toNanos(HEARTBEAT_INTERVAL_MS),
                REPLICATION_LINGER_NANOS > 0
                        ? REPLICATION_LINGER_NANOS
                        : TimeUnit.MILLISECONDS.toNanos(HEARTBEAT_INTERVAL_MS)));
        this.replicationExecutor.scheduleAtFixedRate(this::pumpAllFollowersSafely,
                0, pumpPeriodNanos, TimeUnit.NANOSECONDS);
        this.waitScheduler = Executors.newScheduledThreadPool(1);
        this.electionTimer = new CustomTimer(this::startElection,
                electionTimeoutMs(serverId), TimeUnit.MILLISECONDS);
        this.electionTimer.start();
    }

    private static int electionTimeoutMs(int serverId) {
        if (serverId == PREFERRED_LEADER_ID) {
            return ELECTION_TIMEOUT_BASE_MS / 2;
        }
        // Seeded per node: the jitter spread across nodes is deterministic
        // for a given config seed (constant per node, which is all the
        // jitter is for - separating the nodes' timeouts).
        return ELECTION_TIMEOUT_BASE_MS + new Random(SEED + serverId).nextInt(ELECTION_TIMEOUT_JITTER_MS);
    }

    public void setUpStubs() {
        // When this node's configured host is a literal IPv4 address (the geo
        // latency setup gives every server its own loopback IP), outgoing
        // connections bind to it as their source address, so the OS-level
        // delay rules can identify the (source, destination) server pair.
        // Hostnames like "localhost" are never bound: they may resolve to a
        // different family than the destination picks.
        String ownHost = SERVER_HOSTS.get(serverId);
        final java.net.InetSocketAddress ownSource = ownHost.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")
                ? new java.net.InetSocketAddress(ownHost, 0)
                : null;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i != serverId) {
                io.grpc.netty.NettyChannelBuilder builder = io.grpc.netty.NettyChannelBuilder
                        .forAddress(SERVER_HOSTS.get(i), SERVER_BASE_PORT + (i + 1)).enableRetry()
                        .flowControlWindow(REPLICATION_FLOW_CONTROL_WINDOW_BYTES)
                        .usePlaintext();
                if (ownSource != null) {
                    builder.localSocketPicker(new io.grpc.netty.NettyChannelBuilder.LocalSocketPicker() {
                        @Override
                        public java.net.SocketAddress createSocketAddress(
                                java.net.SocketAddress remoteAddress, io.grpc.Attributes attrs) {
                            return ownSource;
                        }
                    });
                }
                ManagedChannel channel = builder.build();
                ownedChannels.add(channel);
                stubs[i] = RaftGrpc.newStub(channel);
            }
        }
        submitReplicationTask(this::ensureLeaderStreams);
    }

    public void setDropAllServerNetworkTraffic(boolean enabled) {
        this.dropAllServerNetworkTraffic = enabled;
        submitReplicationTask(enabled ? this::closeReplicationStreams : this::ensureLeaderStreams);
    }

    public boolean isDropAllServerNetworkTraffic() {
        return dropAllServerNetworkTraffic;
    }

    private boolean shouldDropServerNetworkTraffic() {
        return dropAllServerNetworkTraffic;
    }

    // ===== Public mechanics for the per-request path =====

    /**
     * Leader-only: append one KV write to the log, apply it to the local view,
     * and return its log index. Replication happens on the heartbeat schedule;
     * callers wait for their write concern via awaitCommitIndex (majority) or
     * awaitReplication (intermediate counts).
     */
    public int appendWrite(String key, String value, String opId) throws NotLeaderException {
        int index;
        lock.writeLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER) {
                throw new NotLeaderException(currentLeader, "server " + serverId + " is not the leader");
            }
            index = log.size();
            LogEntry entry = new LogEntry(index, currentTerm.get(), key, value, opId);
            // State before index: the lock-free fast path in KvClientService
            // serves as soon as lastLogIndex covers its anchor, so the KV view
            // must already contain everything the published index promises.
            kv.applyLocal(entry);
            log.append(entry);
            localLogWaiters.signal(index);
        } finally {
            lock.writeLock().unlock();
        }
        submitReplicationTask(this::pumpAllFollowersSafely);
        return index;
    }

    /** Completes once this node's local log covers the given index. */
    public CompletableFuture<Void> awaitLocalLogIndex(long index) {
        return localLogWaiters.await(index);
    }

    /** Completes once this node's commit index covers the given index. */
    public CompletableFuture<Void> awaitCommitIndex(long index) {
        return commitWaiters.await(index);
    }

    /** Leader-only: completes once the entry is replicated on at least the given number of nodes. */
    public CompletableFuture<Void> awaitReplication(int entryIndex, int requiredReplicas) {
        return replicationWaiters.await(entryIndex, requiredReplicas, this::replicaCount);
    }

    /** Number of nodes (leader included) known to hold the entry at this index. */
    public int replicaCount(int entryIndex) {
        int count = 1;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i != serverId && matchIndex.get(i) >= entryIndex) {
                count++;
            }
        }
        return count;
    }

    public int lastLogIndex() {
        return log.size() - 1;
    }

    public int currentCommitIndex() {
        return commitIndex.get();
    }

    public boolean isLeader() {
        return status == ServerCurrentStatus.LEADER;
    }

    /** Best-known leader id, -1 if unknown. */
    public int leaderIdHint() {
        return currentLeader;
    }

    public int majority() {
        return (NUM_OF_SERVERS / 2) + 1;
    }

    public int nodeId() {
        return serverId;
    }

    // ===== Raft RPC handlers =====

    @Override
    public StreamObserver<AppendEntriesArgument> replicate(
            StreamObserver<AppendEntriesResult> responseObserver) {
        if (!(responseObserver instanceof ServerCallStreamObserver<?>)) {
            throw new IllegalStateException("replication requires a ServerCallStreamObserver");
        }
        @SuppressWarnings("unchecked")
        ServerCallStreamObserver<AppendEntriesResult> outbound =
                (ServerCallStreamObserver<AppendEntriesResult>) responseObserver;
        AtomicBoolean terminal = new AtomicBoolean();
        outbound.disableAutoRequest();
        outbound.setOnCancelHandler(() -> terminal.set(true));

        StreamObserver<AppendEntriesArgument> inbound = new StreamObserver<>() {
            @Override
            public void onNext(AppendEntriesArgument args) {
                if (terminal.get()) {
                    return;
                }
                if (shouldDropServerNetworkTraffic()) {
                    if (terminal.compareAndSet(false, true)) {
                        outbound.onError(Status.UNAVAILABLE
                                .withDescription("Node is configured as failed: replication stream dropped")
                                .asRuntimeException());
                    }
                    return;
                }
                try {
                    outbound.onNext(applyAppendEntries(args));
                } catch (RuntimeException e) {
                    LOG.error("Exception in replication stream on server {}", serverId, e);
                    if (terminal.compareAndSet(false, true)) {
                        outbound.onError(Status.INTERNAL.withDescription(e.toString()).withCause(e)
                                .asRuntimeException());
                    }
                    return;
                }
                if (!terminal.get() && !outbound.isCancelled()) {
                    outbound.request(1);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                terminal.set(true);
            }

            @Override
            public void onCompleted() {
                if (terminal.compareAndSet(false, true)) {
                    outbound.onCompleted();
                }
            }
        };
        outbound.request(1);
        return inbound;
    }

    private AppendEntriesResult applyAppendEntries(AppendEntriesArgument args) {
        int leadersTerm = args.getLeadersTerm(),
                prevLogIndex = args.getPrevLogIndex(),
                prevLogTerm = args.getPrevLogTerm(),
                leadersCommitIndex = args.getLeadersCommit(),
                leaderId = args.getLeadersId();

        lock.writeLock().lock();
        try {
            if (leadersTerm > currentTerm.get()) {
                currentLeader = leaderId;
                currentTerm.updateAndGet(term -> Math.max(term, leadersTerm));
                becomeFollower();
            }
            if (leadersTerm == currentTerm.get()) {
                currentLeader = leaderId;
                // If we're CANDIDATE or LEADER and receive AppendEntries from a
                // valid leader, step down.
                if (status != ServerCurrentStatus.FOLLOWER) {
                    becomeFollower();
                }
            }
            if (leadersTerm < currentTerm.get()) {
                return appendEntriesResult(args, false, -1);
            }
            startTheElectionTimer();

            if (!log.checkIfPrevLogIndexHasPrevLogTerm(prevLogIndex, prevLogTerm)) {
                return appendEntriesResult(args, false, conflictNextIndex(prevLogIndex));
            }

            // Raft 5.3: truncate only at the first entry whose (index, term)
            // conflicts with ours, never on a matching prefix. The leader
            // may replay an acknowledged prefix after a stream reset, so an unconditional
            // truncation at prevLogIndex+1 would wipe and re-append the log on
            // every stale round, and a stale empty probe would wipe it
            // outright - after which the monotonic wait registries would let
            // causal reads serve the emptied view.
            List<LogEntryProto> entries = args.getEntriesList();
            int skip = 0;
            while (skip < entries.size()) {
                LogEntryProto proto = entries.get(skip);
                if (proto.getLogIndex() < log.size() && log.get(proto.getLogIndex()).term == proto.getTerm()) {
                    skip++;
                } else {
                    break;
                }
            }
            if (skip < entries.size()) {
                // Truncate any genuinely conflicting suffix (rebuilds the
                // local KV view), then append the new entries. State before
                // index (see appendWrite): apply each entry to the local KV
                // view before the log publishes its index.
                truncateSuffixAndRebuildLocal(entries.get(skip).getLogIndex());
                for (int i = skip; i < entries.size(); i++) {
                    LogEntryProto proto = entries.get(i);
                    LogEntry entry = new LogEntry(proto.getLogIndex(), proto.getTerm(), proto.getKey(),
                            proto.getValue(), proto.getOpId());
                    kv.applyLocal(entry);
                    log.append(entry);
                }
                localLogWaiters.signal(log.size() - 1);
            }

            if (leadersCommitIndex > commitIndex.get()) {
                int newCommitIndex = Math.min(leadersCommitIndex, log.size() - 1);
                // Committed view before commit index, same ordering rule.
                for (int i = commitIndex.get() + 1; i <= newCommitIndex; i++) {
                    kv.applyCommitted(log.get(i));
                }
                commitIndex.set(newCommitIndex);
                commitWaiters.signal(newCommitIndex);
            }

            return appendEntriesResult(args, true, -1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private AppendEntriesResult appendEntriesResult(AppendEntriesArgument args, boolean success, int nextIndexHint) {
        return AppendEntriesResult.newBuilder()
                .setCurrentTerm(currentTerm.get())
                .setIsSuccessFull(success)
                .setFollowerId(serverId)
                .setSequence(args.getSequence())
                .setNextIndexHint(nextIndexHint)
                .build();
    }

    /** Fast Raft conflict hint: first index of the conflicting follower term, or the log end. */
    private int conflictNextIndex(int prevLogIndex) {
        int size = log.size();
        if (prevLogIndex >= size) {
            return size;
        }
        if (prevLogIndex < 0) {
            return 0;
        }
        int conflictTerm = log.get(prevLogIndex).term;
        int first = prevLogIndex;
        while (first > 0 && log.get(first - 1).term == conflictTerm) {
            first--;
        }
        return first;
    }

    AppendEntriesResult applyAppendEntriesForTest(AppendEntriesArgument args) {
        return applyAppendEntries(args);
    }

    // inside write lock; logIndex is the first log position being truncated
    private void truncateSuffixAndRebuildLocal(int logIndex) {
        if (logIndex >= log.size()) {
            // Nothing is being truncated (the common append-only path).
            return;
        }
        if (logIndex <= commitIndex.get()) {
            // Raft's leader completeness guarantee makes this unreachable: a
            // leader whose consistency check passed can never truncate below
            // the follower's commit index. If it happens, the state machine is
            // corrupt and continuing silently would serve wrong committed data.
            throw new IllegalStateException("attempted to roll back committed entries: truncation at "
                    + logIndex + " but commit index is " + commitIndex.get());
        }
        // Shrink the published log index first, then rebuild the local KV view
        // (committed state plus the surviving uncommitted suffix, in log
        // order). Lock-free readers between the two steps see a low index and
        // wait, which is the conservative direction.
        List<LogEntry> survivingSuffix = log.entriesInRange(commitIndex.get() + 1, logIndex);
        log.truncateAfter(logIndex);
        kv.rebuildLocal(survivingSuffix);
    }

    // in lock
    private boolean isUpToDateCandidateLog(int lastLogTermOfCandidate, int lastLogIndexOfCandidate) {
        int lastLogTermOfCurrentNode = getLastLogTerm(), lastLogIndexOfCurrentNode = getLastLogIndex();
        // deny vote condition
        return !((lastLogTermOfCurrentNode > lastLogTermOfCandidate)
                || ((lastLogTermOfCurrentNode == lastLogTermOfCandidate)
                        && (lastLogIndexOfCurrentNode > lastLogIndexOfCandidate)));
    }

    @Override
    public void requestVote(RequestVoteArguments args, StreamObserver<RequestVoteResult> responseObserver) {
        if (shouldDropServerNetworkTraffic()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: requestVote dropped")
                    .asRuntimeException());
            return;
        }
        int currentTermOfTheCandidate = args.getCandidatesTerm(),
                lastLogIndexOfCandidate = args.getLastLogIndex(),
                lastLogTermOfCandidate = args.getLastLogTerm(),
                candidateId = args.getCandidateId();

        boolean isVoteGranted = true;

        lock.writeLock().lock();
        try {
            if (currentTermOfTheCandidate > currentTerm.get()) {
                currentTerm.updateAndGet(term -> Math.max(term, currentTermOfTheCandidate));
                votedFor = -1;
                becomeFollower();
            }
            if (votedFor != -1 || (this.currentTerm.get() > currentTermOfTheCandidate)
                    || !isUpToDateCandidateLog(lastLogTermOfCandidate, lastLogIndexOfCandidate)) {
                isVoteGranted = false;
            }
            if (isVoteGranted) {
                votedFor = candidateId;
                startTheElectionTimer();
            }
            responseObserver.onNext(RequestVoteResult.newBuilder().setIsVoteGranted(isVoteGranted)
                    .setCurrentTerm(currentTerm.get()).build());
            responseObserver.onCompleted();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ===== Election flow =====

    public int getLastLogIndex() {
        LogEntry last = log.getLastLogEntry();
        return last == null ? -1 : last.index;
    }

    public int getLastLogTerm() {
        LogEntry last = log.getLastLogEntry();
        return last == null ? -1 : last.term;
    }

    private RequestVoteArguments getRequestVoteArgumentsObject() {
        lock.readLock().lock();
        try {
            return RequestVoteArguments.newBuilder().setCandidateId(this.serverId)
                    .setCandidatesTerm(this.currentTerm.get()).setLastLogTerm(this.getLastLogTerm())
                    .setLastLogIndex(this.getLastLogIndex()).build();
        } finally {
            lock.readLock().unlock();
        }
    }

    // all atomic variables, no lock needed
    public boolean handleRequestVoteResult(RequestVoteResult requestVoteResult) {
        if (requestVoteResult.getCurrentTerm() > currentTerm.get()) {
            currentTerm.updateAndGet(term -> Math.max(term, requestVoteResult.getCurrentTerm()));
            return false;
        }
        if (requestVoteResult.getIsVoteGranted()) {
            votes.incrementAndGet();
        }
        return true;
    }

    private void requestForVotes(RequestVoteArguments requestVoteArguments) {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(NUM_OF_SERVERS - 1);

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (i == serverId) {
                continue;
            }
            stubs[i].requestVote(requestVoteArguments, new StreamObserver<RequestVoteResult>() {
                @Override
                public void onNext(RequestVoteResult requestVoteResult) {
                    boolean shouldBecomeLeader = false;
                    boolean shouldBecomeFollower = false;
                    try {
                        lock.writeLock().lock();
                        if (isElectionOver.get()) {
                            latch.countDown();
                            return;
                        }
                        boolean isSuccessful = handleRequestVoteResult(requestVoteResult);
                        if (!isSuccessful) {
                            if (isElectionOver.compareAndSet(false, true)) {
                                shouldBecomeFollower = true;
                            }
                        } else if (votes.get() > (NUM_OF_SERVERS / 2)) {
                            if (isElectionOver.compareAndSet(false, true)) {
                                shouldBecomeLeader = true;
                            }
                        } else {
                            latch.countDown();
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                    if (shouldBecomeLeader) {
                        becomeLeader();
                        while (latch.getCount() > 0) {
                            latch.countDown();
                        }
                    } else if (shouldBecomeFollower) {
                        lock.writeLock().lock();
                        try {
                            becomeFollower();
                        } finally {
                            lock.writeLock().unlock();
                        }
                        while (latch.getCount() > 0) {
                            latch.countDown();
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                }
            });
        }
        try {
            boolean success = latch.await(800, TimeUnit.MILLISECONDS);
            lock.writeLock().lock();
            try {
                if (!success && isElectionOver.compareAndSet(false, true)
                        && status != ServerCurrentStatus.FOLLOWER) {
                    becomeFollower();
                }
            } finally {
                lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void startElection() {
        if (shouldDropServerNetworkTraffic()) {
            return;
        }
        lock.writeLock().lock();
        RequestVoteArguments requestVoteArguments;
        try {
            this.status = ServerCurrentStatus.CANDIDATE;
            currentTerm.incrementAndGet();
            startTheElectionTimer();
            votes.set(1);
            votedFor = serverId;
            isElectionOver.set(false);
            requestVoteArguments = getRequestVoteArgumentsObject();
        } finally {
            lock.writeLock().unlock();
        }
        requestForVotes(requestVoteArguments);
    }

    private void becomeLeader() {
        lock.writeLock().lock();
        try {
            // A candidate that became follower via requestVote in between must
            // not become leader.
            if (status != ServerCurrentStatus.CANDIDATE) {
                return;
            }
            electionTimer.stop();
            reinitialiseIndexes();
            this.status = ServerCurrentStatus.LEADER;
            this.currentLeader = this.serverId;
        } finally {
            lock.writeLock().unlock();
        }
        submitReplicationTask(() -> {
            closeReplicationStreams();
            ensureLeaderStreams();
            pumpAllFollowersSafely();
        });
    }

    // should be inside write lock
    private void becomeFollower() {
        if (status == ServerCurrentStatus.LEADER || status == ServerCurrentStatus.CANDIDATE) {
            startTheElectionTimer();
        }
        status = ServerCurrentStatus.FOLLOWER;
        votedFor = -1;
        // A node that is no longer leader can never confirm leadership or
        // satisfy leader-side replication waits.
        failPendingLeadershipConfirmations();
        replicationWaiters.failAll(new NotLeaderException(currentLeader, "stepped down from leadership"));
        submitReplicationTask(this::closeReplicationStreams);
    }

    // already in writeLock
    private void reinitialiseIndexes() {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            nextIndex.set(i, log.size());
            matchIndex.set(i, -1);
        }
    }

    // ===== Replication =====

    // Batch-fill visibility: the leader prints one line per second summarizing
    // the ordered replication messages it sent, so a run shows whether batches
    // are full (pipeline-bound) or starved (intake-bound).
    private final AtomicLong batchRpcsLastSecond = new AtomicLong();
    // Cumulative, never reset: how many replication messages this leader has
    // sent. The per-second counters above are cleared by the printer, so they
    // cannot answer "how many messages did this workload cost".
    private final AtomicLong replicationMessagesSent = new AtomicLong();
    private final AtomicLong batchEntriesLastSecond = new AtomicLong();
    private final AtomicLong batchBytesLastSecond = new AtomicLong();
    private final AtomicLong batchMaxLastSecond = new AtomicLong();
    private final AtomicLong batchMaxBytesLastSecond = new AtomicLong();
    private final AtomicLong batchLastPrintMs = new AtomicLong(System.currentTimeMillis());

    private void recordAppendEntriesBatch(int entries, int serializedBytes) {
        replicationMessagesSent.incrementAndGet();
        batchRpcsLastSecond.incrementAndGet();
        batchEntriesLastSecond.addAndGet(entries);
        batchBytesLastSecond.addAndGet(serializedBytes);
        batchMaxLastSecond.accumulateAndGet(entries, Math::max);
        batchMaxBytesLastSecond.accumulateAndGet(serializedBytes, Math::max);
        long now = System.currentTimeMillis();
        long last = batchLastPrintMs.get();
        if (now - last >= 1000 && batchLastPrintMs.compareAndSet(last, now)) {
            long rpcs = batchRpcsLastSecond.getAndSet(0);
            long sum = batchEntriesLastSecond.getAndSet(0);
            long bytes = batchBytesLastSecond.getAndSet(0);
            long max = batchMaxLastSecond.getAndSet(0);
            long maxBytes = batchMaxBytesLastSecond.getAndSet(0);
            if (sum > 0) {
                System.out.printf(
                        "[replication] stream last %ds: messages=%d entries=%d avgBatch=%.0f maxBatch=%d"
                                + " bytes=%.2fMB avgMessage=%.1fKB maxMessage=%.1fKB cap=%d%n",
                        Math.round((now - last) / 1000.0), rpcs, sum,
                        (double) sum / Math.max(1, rpcs), max,
                        bytes / 1e6, bytes / 1e3 / Math.max(1, rpcs), maxBytes / 1e3,
                        MAX_ENTRIES_PER_REPLICATION_BATCH);
            }
        }
    }

    private void submitReplicationTask(Runnable task) {
        if (replicationExecutor.isShutdown()) {
            return;
        }
        try {
            replicationExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            if (!replicationExecutor.isShutdown()) {
                throw e;
            }
        }
    }

    private void pumpAllFollowersSafely() {
        try {
            if (shouldDropServerNetworkTraffic() || status != ServerCurrentStatus.LEADER) {
                return;
            }
            ensureLeaderStreams();
            for (ReplicationPipeline pipeline : replicationPipelines) {
                if (pipeline != null) {
                    pumpFollower(pipeline);
                }
            }
        } catch (RuntimeException e) {
            LOG.error("Replication pipeline failed on leader {}", serverId, e);
        }
    }

    private void ensureLeaderStreams() {
        if (shouldDropServerNetworkTraffic() || status != ServerCurrentStatus.LEADER) {
            return;
        }
        for (ReplicationPipeline pipeline : replicationPipelines) {
            if (pipeline != null && pipeline.outbound == null && !pipeline.connecting
                    && stubs[pipeline.followerId] != null) {
                openReplicationStream(pipeline);
            }
        }
    }

    private void openReplicationStream(ReplicationPipeline pipeline) {
        if (shouldDropServerNetworkTraffic() || status != ServerCurrentStatus.LEADER
                || stubs[pipeline.followerId] == null || pipeline.outbound != null || pipeline.connecting) {
            return;
        }
        pipeline.connecting = true;
        pipeline.generation++;
        long generation = pipeline.generation;
        int leaderTerm = currentTerm.get();
        pipeline.resetSpeculativeState(nextIndex.get(pipeline.followerId), leaderTerm);
        AtomicReference<ClientCallStreamObserver<AppendEntriesArgument>> requestStreamRef = new AtomicReference<>();

        ClientResponseObserver<AppendEntriesArgument, AppendEntriesResult> responses = new ClientResponseObserver<>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<AppendEntriesArgument> requestStream) {
                requestStreamRef.set(requestStream);
                requestStream.setOnReadyHandler(() -> submitReplicationTask(
                        () -> handleReplicationStreamReady(pipeline.followerId, generation)));
            }

            @Override
            public void onNext(AppendEntriesResult result) {
                submitReplicationTask(() -> handleReplicationResponse(pipeline.followerId, generation, result));
            }

            @Override
            public void onError(Throwable throwable) {
                submitReplicationTask(() -> handleReplicationStreamClosed(
                        pipeline.followerId, generation, throwable));
            }

            @Override
            public void onCompleted() {
                submitReplicationTask(() -> handleReplicationStreamClosed(
                        pipeline.followerId, generation, null));
            }
        };

        stubs[pipeline.followerId].replicate(responses);
        ClientCallStreamObserver<AppendEntriesArgument> requestStream = requestStreamRef.get();
        if (requestStream == null) {
            pipeline.connecting = false;
            throw new IllegalStateException("gRPC did not expose the replication request stream for follower "
                    + pipeline.followerId);
        }
        if (pipeline.generation != generation || status != ServerCurrentStatus.LEADER) {
            requestStream.onCompleted();
            pipeline.connecting = false;
            return;
        }
        pipeline.outbound = requestStream;
        pipeline.connecting = false;
        pumpFollower(pipeline);
    }

    private void handleReplicationStreamReady(int followerId, long generation) {
        ReplicationPipeline pipeline = replicationPipelines[followerId];
        if (pipeline != null && pipeline.generation == generation) {
            pumpFollower(pipeline);
        }
    }

    private void pumpFollower(ReplicationPipeline pipeline) {
        ClientCallStreamObserver<AppendEntriesArgument> outbound = pipeline.outbound;
        if (outbound == null || shouldDropServerNetworkTraffic()
                || status != ServerCurrentStatus.LEADER || pipeline.term != currentTerm.get()) {
            return;
        }
        while (pipeline.inFlight.size() < MAX_INFLIGHT_REPLICATION_BATCHES_PER_FOLLOWER
                && outbound.isReady()) {
            AppendEntriesArgument request = buildNextReplicationMessage(pipeline);
            if (request == null) {
                return;
            }
            int entries = request.getEntriesCount();
            InflightBatch batch = new InflightBatch(request.getSequence(),
                    request.getPrevLogIndex() + 1,
                    request.getPrevLogIndex() + 1 + entries,
                    request.getLeadersTerm(), System.nanoTime());
            pipeline.inFlight.addLast(batch);
            maxObservedReplicationInflight.accumulateAndGet(
                    pipeline.followerId, pipeline.inFlight.size(), Math::max);
            maxObservedReplicationBatchEntries.accumulateAndGet(entries, Math::max);
            recordAppendEntriesBatch(entries, request.getSerializedSize());
            try {
                outbound.onNext(request);
            } catch (RuntimeException e) {
                restartReplicationStream(pipeline, nextIndex.get(pipeline.followerId), e);
                return;
            }
        }
    }

    private AppendEntriesArgument buildNextReplicationMessage(ReplicationPipeline pipeline) {
        lock.readLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER || pipeline.term != currentTerm.get()) {
                return null;
            }
            int logSize = log.size();
            if (pipeline.nextSendIndex > logSize) {
                throw new IllegalStateException("replication cursor " + pipeline.nextSendIndex
                        + " exceeds leader log size " + logSize);
            }
            boolean hasData = pipeline.nextSendIndex < logSize;
            long now = System.nanoTime();
            if (!hasData && (!pipeline.inFlight.isEmpty()
                    || now - pipeline.lastSendNanos < TimeUnit.MILLISECONDS.toNanos(HEARTBEAT_INTERVAL_MS))) {
                return null;
            }
            // Hold a short batch back until the linger has elapsed, whatever woke
            // the pump - an acknowledgement, the stream turning writable, a fresh
            // append, or the periodic tick. A batch at the cap is never delayed,
            // so catch-up and overload both run at full speed; only the trickle
            // case waits, and it waits at most one linger.
            if (hasData && logSize - pipeline.nextSendIndex < MAX_ENTRIES_PER_REPLICATION_BATCH
                    && now - pipeline.lastSendNanos < REPLICATION_LINGER_NANOS) {
                return null;
            }

            int start = pipeline.nextSendIndex;
            int end = hasData
                    ? Math.min(logSize, start + MAX_ENTRIES_PER_REPLICATION_BATCH)
                    : start;
            int prevLogIndex = start - 1;
            int prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).term : -1;
            List<LogEntryProto> protoEntries = new ArrayList<>(Math.max(0, end - start));
            for (LogEntry entry : log.entriesInRange(start, end)) {
                protoEntries.add(LogEntryProto.newBuilder()
                        .setLogIndex(entry.index).setTerm(entry.term)
                        .setKey(entry.key).setValue(entry.value).setOpId(entry.opId)
                        .build());
            }
            long sequence = ++pipeline.nextSequence;
            pipeline.nextSendIndex = end;
            pipeline.lastSendNanos = now;
            return AppendEntriesArgument.newBuilder()
                    .setLeadersTerm(currentTerm.get())
                    .setLeadersId(serverId)
                    .setLeadersCommit(commitIndex.get())
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(protoEntries)
                    .setSequence(sequence)
                    .build();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void handleReplicationResponse(int followerId, long generation, AppendEntriesResult result) {
        ReplicationPipeline pipeline = replicationPipelines[followerId];
        if (pipeline == null || pipeline.generation != generation) {
            return;
        }
        InflightBatch batch = pipeline.inFlight.peekFirst();
        if (batch == null || result.getFollowerId() != followerId
                || result.getSequence() != batch.sequence()) {
            restartReplicationStream(pipeline, nextIndex.get(followerId),
                    new IllegalStateException("out-of-order replication acknowledgement from follower " + followerId));
            return;
        }
        pipeline.inFlight.removeFirst();

        int retryFrom = -1;
        lock.writeLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER || batch.term() != currentTerm.get()) {
                return;
            }
            if (result.getCurrentTerm() > currentTerm.get()) {
                currentTerm.set(result.getCurrentTerm());
                becomeFollower();
                return;
            }
            if (!result.getIsSuccessFull()) {
                int minimum = matchIndex.get(followerId) + 1;
                retryFrom = Math.max(minimum, Math.min(result.getNextIndexHint(), log.size()));
                nextIndex.set(followerId, retryFrom);
            } else {
                int acknowledged = batch.acknowledgedIndex();
                if (acknowledged > matchIndex.get(followerId)) {
                    matchIndex.set(followerId, acknowledged);
                    nextIndex.set(followerId, acknowledged + 1);
                    advanceCommitIndexIfPossible();
                    replicationWaiters.evaluate(this::replicaCount);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (retryFrom >= 0) {
            restartReplicationStream(pipeline, retryFrom, null);
            return;
        }
        if (result.getCurrentTerm() <= batch.term()) {
            recordLeadershipAck(followerId, batch.sendStartNanos(), batch.term());
        }
        pumpFollower(pipeline);
    }

    // Called with the Raft write lock held.
    private void advanceCommitIndexIfPossible() {
        int previous = commitIndex.get();
        int candidate = getCommitIndexIfPossibleEarlyExitMethod();
        if (candidate <= previous) {
            return;
        }
        for (int i = previous + 1; i <= candidate; i++) {
            kv.applyCommitted(log.get(i));
        }
        commitIndex.set(candidate);
        commitWaiters.signal(candidate);
    }

    private void handleReplicationStreamClosed(int followerId, long generation, Throwable error) {
        ReplicationPipeline pipeline = replicationPipelines[followerId];
        if (pipeline == null || pipeline.generation != generation) {
            return;
        }
        if (error != null && status == ServerCurrentStatus.LEADER && !shouldDropServerNetworkTraffic()) {
            LOG.debug("Replication stream to follower {} closed: {}", followerId, error.toString());
        }
        invalidateReplicationStream(pipeline, nextIndex.get(followerId), false);
        if (status == ServerCurrentStatus.LEADER && !shouldDropServerNetworkTraffic()
                && !replicationExecutor.isShutdown()) {
            replicationExecutor.schedule(() -> openReplicationStream(pipeline),
                    HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void restartReplicationStream(ReplicationPipeline pipeline, int confirmedNextIndex, Throwable cause) {
        if (cause != null) {
            LOG.debug("Restarting replication stream to follower {}: {}",
                    pipeline.followerId, cause.toString());
        }
        invalidateReplicationStream(pipeline, confirmedNextIndex, true);
        if (status == ServerCurrentStatus.LEADER && !shouldDropServerNetworkTraffic()) {
            submitReplicationTask(() -> openReplicationStream(pipeline));
        }
    }

    private void invalidateReplicationStream(ReplicationPipeline pipeline,
            int confirmedNextIndex, boolean completeOutbound) {
        ClientCallStreamObserver<AppendEntriesArgument> old = pipeline.outbound;
        pipeline.generation++;
        pipeline.outbound = null;
        pipeline.connecting = false;
        pipeline.resetSpeculativeState(confirmedNextIndex, currentTerm.get());
        if (completeOutbound && old != null) {
            try {
                old.onCompleted();
            } catch (RuntimeException e) {
                LOG.debug("Closing replication stream to follower {} failed: {}",
                        pipeline.followerId, e.toString());
            }
        }
    }

    private void closeReplicationStreams() {
        for (ReplicationPipeline pipeline : replicationPipelines) {
            if (pipeline != null) {
                invalidateReplicationStream(pipeline, nextIndex.get(pipeline.followerId), true);
            }
        }
    }

    int maxObservedReplicationInflight(int followerId) {
        return maxObservedReplicationInflight.get(followerId);
    }

    /** Replication messages sent since startup, for tests and diagnostics. */
    long replicationMessagesSent() {
        return replicationMessagesSent.get();
    }

    int maxObservedReplicationBatchEntries() {
        return maxObservedReplicationBatchEntries.get();
    }

    // should be inside a lock; walks down from the max matchIndex, so it is
    // O(n) in practice because followers trail the leader by a few entries
    private int getCommitIndexIfPossibleEarlyExitMethod() {
        int maxMatchIndex = -1;
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            maxMatchIndex = Math.max(maxMatchIndex, matchIndex.get(i));
        }
        for (int idx = maxMatchIndex; idx > commitIndex.get(); idx--) {
            int count = 0;
            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (matchIndex.get(i) >= idx) {
                    count++;
                }
            }
            // count is followers only (the leader's own matchIndex stays -1),
            // so followers >= N/2 plus the leader itself is a majority. Only
            // current-term entries may be committed by counting.
            if (count >= (NUM_OF_SERVERS / 2) && log.get(idx).term == currentTerm.get()) {
                return idx;
            }
        }
        return -1;
    }

    // this resets and starts the timer again
    private void startTheElectionTimer() {
        this.electionTimer.reset();
    }

    // ===== ReadIndex leadership confirmation =====

    /** Thrown when a leader-only operation is invoked on a non-leader. */
    public static final class NotLeaderException extends Exception {
        /** Best-known leader id at the time of rejection, -1 if unknown. */
        public final int leaderId;

        public NotLeaderException(int leaderId, String message) {
            super(message);
            this.leaderId = leaderId;
        }
    }

    private static final long CONFIRM_LEADERSHIP_TIMEOUT_MS = 1000;

    private static final class LeadershipConfirmation {
        final int term;
        final int readIndex;
        final long registeredAtNanos;
        final Set<Integer> ackedFollowers = ConcurrentHashMap.newKeySet();
        final CompletableFuture<Integer> future = new CompletableFuture<>();

        LeadershipConfirmation(int term, int readIndex, long registeredAtNanos) {
            this.term = term;
            this.readIndex = readIndex;
            this.registeredAtNanos = registeredAtNanos;
        }
    }

    private final ConcurrentLinkedQueue<LeadershipConfirmation> pendingLeadershipConfirmations = new ConcurrentLinkedQueue<>();

    /**
     * ReadIndex (linearizable reads without log entries): snapshot the commit
     * index, then confirm this node is still the leader by observing a majority
     * of followers acknowledge an AppendEntries round that started after this
     * call. The returned future completes with the snapshotted commit index;
     * the caller can then serve the read from local committed state. No log
     * entry is created, and the confirmation rides the regular replication
     * heartbeats already in flight.
     *
     * The committed KV view is applied in the same critical sections that
     * advance the commit index, so once the future completes the local
     * committed view already covers the returned index and no apply-wait is
     * needed.
     */
    public CompletableFuture<Integer> confirmLeadership() {
        lock.readLock().lock();
        try {
            if (status != ServerCurrentStatus.LEADER) {
                return CompletableFuture.failedFuture(
                        new NotLeaderException(currentLeader, "server " + serverId + " is not the leader"));
            }
            int readIndex = commitIndex.get();
            if (NUM_OF_SERVERS == 1) {
                return CompletableFuture.completedFuture(readIndex);
            }
            LeadershipConfirmation confirmation = new LeadershipConfirmation(
                    currentTerm.get(), readIndex, System.nanoTime());
            pendingLeadershipConfirmations.add(confirmation);
            waitScheduler.schedule(() -> {
                if (pendingLeadershipConfirmations.remove(confirmation)) {
                    confirmation.future.completeExceptionally(new TimeoutException(
                            "no majority heartbeat round within " + CONFIRM_LEADERSHIP_TIMEOUT_MS + " ms"));
                }
            }, CONFIRM_LEADERSHIP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return confirmation.future;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void recordLeadershipAck(int followerId, long sendStartNanos, int termAtSend) {
        if (pendingLeadershipConfirmations.isEmpty()) {
            return;
        }
        for (LeadershipConfirmation confirmation : pendingLeadershipConfirmations) {
            // Only rounds started after registration prove leadership at (or
            // after) the moment the read index was snapshotted.
            if (confirmation.term != termAtSend || sendStartNanos < confirmation.registeredAtNanos) {
                continue;
            }
            confirmation.ackedFollowers.add(followerId);
            // +1 counts this node itself.
            if (confirmation.ackedFollowers.size() + 1 >= majority()
                    && pendingLeadershipConfirmations.remove(confirmation)) {
                confirmation.future.complete(confirmation.readIndex);
            }
        }
    }

    private void failPendingLeadershipConfirmations() {
        LeadershipConfirmation confirmation;
        while ((confirmation = pendingLeadershipConfirmations.poll()) != null) {
            confirmation.future.completeExceptionally(
                    new NotLeaderException(currentLeader, "stepped down from leadership"));
        }
    }

    /**
     * RPC handler: a follower asks this node (believing it the leader) for a
     * confirmed read index. Runs the same ReadIndex confirmation as a local
     * linearizable read; a non-leader answers success=false with its best
     * leader hint instead of an error, so the caller can redirect.
     */
    @Override
    public void confirmReadIndex(ReadIndexRequest request, StreamObserver<ReadIndexResult> responseObserver) {
        if (shouldDropServerNetworkTraffic()) {
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription("Node is configured as failed: confirmReadIndex dropped")
                    .asRuntimeException());
            return;
        }
        confirmLeadership().whenComplete((readIndex, error) -> {
            ReadIndexResult result = (error == null)
                    ? ReadIndexResult.newBuilder().setSuccess(true).setReadIndex(readIndex)
                            .setLeaderId(serverId).build()
                    : ReadIndexResult.newBuilder().setSuccess(false).setReadIndex(-1)
                            .setLeaderId(currentLeader == serverId ? -1 : currentLeader).build();
            synchronized (responseObserver) {
                try {
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    LOG.debug("Dropping confirmReadIndex reply on server {}: {}", serverId, e.toString());
                }
            }
        });
    }

    // ===== Batched follower read index =====

    // At most one read-index round is in flight per follower; requests that
    // arrive before a round's send share its returned index (their
    // linearization point, the confirmation, happens after their arrival).
    // Requests arriving while a round is in flight join the next round.
    // Callers must never orTimeout the shared future directly; derive first.
    private CompletableFuture<Integer> inFlightReadIndexRound;   // guarded by readIndexRoundLock
    private CompletableFuture<Integer> queuedReadIndexRound;     // guarded by readIndexRoundLock
    private final Object readIndexRoundLock = new Object();
    final AtomicInteger readIndexRoundsSent = new AtomicInteger(); // test observability

    /**
     * Follower-side half of follower linearizable reads: obtain a confirmed
     * read index from the current leader, batching concurrent callers into
     * shared rounds. The caller must then wait for this node's own commit
     * index to reach the returned index before serving from the committed
     * view. Fails with NotLeaderException when no leader is known or the
     * presumed leader has stepped down.
     */
    public CompletableFuture<Integer> readIndexFromLeader() {
        if (status == ServerCurrentStatus.LEADER) {
            return confirmLeadership();
        }
        CompletableFuture<Integer> round;
        boolean sendNow = false;
        synchronized (readIndexRoundLock) {
            if (inFlightReadIndexRound != null) {
                if (queuedReadIndexRound == null) {
                    queuedReadIndexRound = new CompletableFuture<>();
                }
                round = queuedReadIndexRound;
            } else {
                inFlightReadIndexRound = new CompletableFuture<>();
                round = inFlightReadIndexRound;
                sendNow = true;
            }
        }
        if (sendNow) {
            sendReadIndexRound(round);
        }
        return round;
    }

    private void sendReadIndexRound(CompletableFuture<Integer> round) {
        int leaderId = currentLeader;
        if (status == ServerCurrentStatus.LEADER) {
            confirmLeadership().whenComplete((readIndex, error) -> finishReadIndexRound(round, readIndex, error));
            return;
        }
        if (leaderId < 0 || leaderId == serverId || dropAllServerNetworkTraffic) {
            finishReadIndexRound(round, null,
                    new NotLeaderException(-1, "server " + serverId + " knows no leader to ask for a read index"));
            return;
        }
        readIndexRoundsSent.incrementAndGet();
        stubs[leaderId].confirmReadIndex(ReadIndexRequest.newBuilder().setRequesterId(serverId).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(ReadIndexResult result) {
                        if (result.getSuccess()) {
                            finishReadIndexRound(round, result.getReadIndex(), null);
                        } else {
                            finishReadIndexRound(round, null, new NotLeaderException(result.getLeaderId(),
                                    "server " + leaderId + " is no longer the leader"));
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        finishReadIndexRound(round, null, t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    private void finishReadIndexRound(CompletableFuture<Integer> round, Integer readIndex, Throwable error) {
        // Promote the queued round before completing this one, so late
        // callers can never join a round that has already resolved.
        CompletableFuture<Integer> next;
        synchronized (readIndexRoundLock) {
            inFlightReadIndexRound = queuedReadIndexRound;
            queuedReadIndexRound = null;
            next = inFlightReadIndexRound;
        }
        if (error != null) {
            round.completeExceptionally(error);
        } else {
            round.complete(readIndex);
        }
        if (next != null) {
            sendReadIndexRound(next);
        }
    }

    /**
     * Stop all background activity (election timer, replication scheduler,
     * worker pool) and close every gRPC channel this node opened. Used by
     * tests and orderly teardown; the node is unusable afterwards.
     */
    public void shutdown() {
        dropAllServerNetworkTraffic = true;
        electionTimer.stop();
        electionTimer.shutdown();
        failPendingLeadershipConfirmations();
        replicationWaiters.failAll(new NotLeaderException(-1, "node shut down"));
        submitReplicationTask(this::closeReplicationStreams);
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            replicationExecutor.shutdownNow();
        }
        waitScheduler.shutdownNow();
        executorService.shutdownNow();
        synchronized (ownedChannels) {
            for (ManagedChannel channel : ownedChannels) {
                channel.shutdownNow();
            }
        }
    }
}
