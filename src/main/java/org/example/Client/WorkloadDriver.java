package org.example.Client;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.example.Utility.ExperimentConfig;
import org.example.Utility.KeySampler;
import org.example.raft.AdminGrpc;
import org.example.raft.AdminStatusReply;
import org.example.raft.AdminStatusRequest;
import org.example.raft.SetDropTrafficRequest;
import org.example.raft.ShutdownRequest;
import org.example.raft.ReadLevel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * Experiment entry point for the workload side: drives the phased workload
 * through per-application KvSessionClients over persistent framed connections against a
 * cluster of server processes (org.example.Server.ServerNode, one per node).
 * Cluster orchestration that used to happen on in-process ServerImpl
 * references - leader detection, failure injection, teardown - goes over the
 * Admin service instead. All comparison metrics are client-side
 * (ClientMetricsTracker).
 */
public class WorkloadDriver {

    // ===== Configuration (loaded from YAML/JSON at startup, see ExperimentConfig) =====
    public static int NUM_OF_SERVERS;
    private static int SERVER_BASE_PORT;
    private static int CLIENT_BASE_PORT;
    private static List<String> SERVER_HOSTS;

    private static boolean ENABLE_NODE_NETWORK_FAILURE;
    private static int FAILED_NODE_ID;

    private static FailurePlan FAILURE_PLAN;

    private static int RTT_WINDOW_SIZE;
    private static int CLIENT_RETRY_LIMIT;
    private static int CLIENT_LOST_TIMEOUT_MS;
    private static int INGRESS_CONNECTIONS_PER_SERVER;

    // Client sites (geo.clientSites): every session belongs to one site and
    // rides that site's connection pools. When geo is enabled each site's
    // connections bind their own 127.0.2.x source IP so the netem rules can
    // apply per-site delays; when disabled the sites differ only in name.
    private static List<String> CLIENT_SITE_NAMES;
    private static boolean GEO_ENABLED;

    // Number of applications, derived from the registered SLAs (the config
    // loader guarantees applicationIds are exactly 1..N).
    private static int NUM_APPLICATIONS;
    private static final int TICK_MS = 100;

    private static final long CLUSTER_REACHABLE_TIMEOUT_MS = 30_000;
    private static final long LEADER_ELECTION_TIMEOUT_MS = 15_000;
    private static final long ADMIN_RPC_DEADLINE_MS = 2_000;

    private static KeySampler keySampler;

    // ===== Workload phases =====

    /** One slice of a phase's traffic: which app, read or write, which SLA. */
    static final class MixEntry {
        final int applicationId;
        final boolean isRead;
        final int slaId;
        final double weight;

        MixEntry(int applicationId, boolean isRead, int slaId, double weight) {
            this.applicationId = applicationId;
            this.isRead = isRead;
            this.slaId = slaId;
            this.weight = weight;
        }
    }

    static class Phase {
        final String name;
        final int durationSeconds;
        final int totalTPS;
        final String mixName;
        final List<MixEntry> mix;
        private final double[] cumulativeWeights;
        private final double totalWeight;

        Phase(String name, int durationSeconds, int totalTPS, String mixName, List<MixEntry> mix) {
            this.name = name;
            this.durationSeconds = durationSeconds;
            this.totalTPS = totalTPS;
            this.mixName = mixName;
            this.mix = List.copyOf(mix);
            this.cumulativeWeights = new double[mix.size()];
            double running = 0;
            for (int i = 0; i < mix.size(); i++) {
                running += mix.get(i).weight;
                cumulativeWeights[i] = running;
            }
            this.totalWeight = running;
        }

        MixEntry sample(Random random) {
            double r = random.nextDouble() * totalWeight;
            for (int i = 0; i < cumulativeWeights.length; i++) {
                if (r < cumulativeWeights[i]) {
                    return mix.get(i);
                }
            }
            return mix.get(mix.size() - 1);
        }

        /** Fraction of this phase's traffic that is reads, derived from the weights. */
        double readShare() {
            double reads = 0;
            for (MixEntry entry : mix) {
                if (entry.isRead) {
                    reads += entry.weight;
                }
            }
            return reads / totalWeight;
        }
    }

    private static final List<Phase> PHASES = new ArrayList<>();
    private static final AtomicInteger currentPhaseIndex = new AtomicInteger(0);

    private static long TOTAL_EXPERIMENT_DURATION_MS;
    private static volatile long experimentStartTime;
    private static volatile boolean experimentRunning = true;
    private static volatile Phase currentPhase = null;
    private static volatile long phaseEndTime = 0;
    private static final Object phaseLock = new Object();

    private static AdminGrpc.AdminBlockingStub[] adminStubs;
    private static final List<ManagedChannel> adminChannels = new ArrayList<>();
    // appSessions[app][session]: independent session clients per application,
    // drawn uniformly per request. Every session carries its own full client
    // state (session index, per-key floors, RTT windows, Pileus windows);
    // only the framed transport and metrics ledger are shared.
    private static KvSessionClient[][] appSessions;
    // The wire: the framed KV protocol against the Java store, or the MongoDB
    // wire protocol against the patched replica set (config transport).
    private static KvTransport transport;
    // Cluster administration when the transport is MongoDB; null otherwise
    // (the Java store is administered over the gRPC admin stubs below).
    private static MongoClusterControl mongoControl;
    private static int SESSIONS_PER_APPLICATION;
    // Injection threads; sessions are sharded across them so every session
    // stays driven by exactly one thread (a session is one logical actor).
    private static int INJECTOR_THREADS;
    private static ClientMode CLIENT_MODE;
    private static double EXPLORATION_FRACTION;
    // Base PRNG seed (config `seed`); every Random here derives from it so
    // two runs of the same config face the same request stream.
    private static long SEED = 42;
    private static boolean ADMISSION_AWARE_ROUTING;
    private static double ADMIT_RATE_GAMMA;
    private static boolean FOLLOWER_LIN_READS;
    private static Map<Integer, Map<Integer, List<org.example.Utility.RungScorer.Rung>>> readSlasByApp;
    private static Map<Integer, Map<Integer, List<org.example.Utility.RungScorer.Rung>>> writeSlasByApp;

    static void applyConfig(ExperimentConfig config) {
        NUM_OF_SERVERS = config.cluster.numServers;
        SERVER_BASE_PORT = config.cluster.serverBasePort;
        CLIENT_BASE_PORT = config.cluster.clientBasePort;
        SERVER_HOSTS = List.copyOf(config.cluster.serverHosts);

        ENABLE_NODE_NETWORK_FAILURE = config.nodeFailure.enabled;
        FAILED_NODE_ID = config.nodeFailure.failedNodeId;

        FAILURE_PLAN = FailurePlan.of(config);

        RTT_WINDOW_SIZE = config.client.rttWindowSize;
        CLIENT_RETRY_LIMIT = config.client.retryLimit;
        CLIENT_LOST_TIMEOUT_MS = config.client.lostTimeoutMs;
        INGRESS_CONNECTIONS_PER_SERVER = config.client.ingressConnectionsPerServer;
        CLIENT_SITE_NAMES = config.geo.clientSites.stream()
                .map(site -> site.name).toList();
        GEO_ENABLED = config.geo.enabled;
        CLIENT_MODE = ClientMode.fromConfig(config.mode);
        SEED = config.seed;
        EXPLORATION_FRACTION = config.client.explorationFraction;
        ADMISSION_AWARE_ROUTING = config.client.admissionAwareRouting;
        ADMIT_RATE_GAMMA = config.client.admitRateGamma;
        FOLLOWER_LIN_READS = config.server.followerLinearizableReads;

        SESSIONS_PER_APPLICATION = config.workload.sessionsPerApplication;
        INJECTOR_THREADS = config.workload.injectorThreads;
        NUM_APPLICATIONS = config.slas.size();

        PHASES.clear();
        for (ExperimentConfig.PhaseConfig p : config.workload.phases) {
            // The config loader validated the mix reference and every entry
            // in it (registered SLA, applicationId in 1..N, positive weight).
            List<MixEntry> mix = new ArrayList<>();
            for (ExperimentConfig.SlaShare share : config.workload.mixes.get(p.mix)) {
                mix.add(new MixEntry(share.applicationId, share.type.equals("read"), share.slaId, share.weight));
            }
            PHASES.add(new Phase(p.name, p.durationSeconds, p.totalTPS, p.mix, mix));
        }
        currentPhase = PHASES.get(0);

        // The full rung tables the clients target and grade against (the
        // application's own registration, mirrored client-side).
        readSlasByApp = new HashMap<>();
        writeSlasByApp = new HashMap<>();
        for (ExperimentConfig.AppSlas app : config.slas) {
            Map<Integer, List<org.example.Utility.RungScorer.Rung>> readTables = new HashMap<>();
            for (ExperimentConfig.Sla sla : app.read) {
                readTables.put(sla.slaId, sla.rungs.stream()
                        .map(r -> new org.example.Utility.RungScorer.Rung(
                                ReadLevel.valueOf(r.level).getNumber(), r.latencyMs, r.profit))
                        .toList());
            }
            Map<Integer, List<org.example.Utility.RungScorer.Rung>> writeTables = new HashMap<>();
            for (ExperimentConfig.Sla sla : app.write) {
                writeTables.put(sla.slaId, sla.rungs.stream()
                        .map(r -> new org.example.Utility.RungScorer.Rung(r.concern, r.latencyMs, r.profit))
                        .toList());
            }
            readSlasByApp.put(app.applicationId, readTables);
            writeSlasByApp.put(app.applicationId, writeTables);
        }

        keySampler = config.workload.keyDistribution.equals("zipfian")
                ? KeySampler.zipfian(config.workload.keySpace, config.workload.zipfianExponent)
                : KeySampler.uniform(config.workload.keySpace);

        TOTAL_EXPERIMENT_DURATION_MS = PHASES.stream().mapToLong(p -> p.durationSeconds).sum() * 1000L;
    }

    public static void main(String[] args) throws InterruptedException {
        Path configPath = Path.of(args.length > 0 ? args[0] : "config_local.yaml");
        ExperimentConfig config = ExperimentConfig.load(configPath);
        applyConfig(config);
        System.out.println("Loaded config from " + configPath.toAbsolutePath());

        clearCSVFiles();

        // One connection pool per (site, server); when geo is enabled every
        // site binds its own source IP so the delay rules can match per-site
        // (source, destination) pairs - 127.0.2.x on a single-host run, or the
        // routable aliases in geo.clientSiteBindHosts on a distributed one.
        // Session s of every application lives at site s mod #sites, so
        // sessions spread evenly (the loader rejects configs where they would
        // not divide).
        int numSites = CLIENT_SITE_NAMES.size();
        List<String> siteBindHosts = new ArrayList<>();
        for (int site = 0; site < numSites; site++) {
            siteBindHosts.add(GEO_ENABLED ? config.siteBindHost(site) : null);
        }

        if (config.mongodbTransport()) {
            // The patched replica set: wait for it, push the Chameleon parameters and the
            // SLA table, and poll its serverStatus into occupancy_<n>.csv (MONGODB_PORT.md).
            mongoControl = new MongoClusterControl(config);
            mongoControl.waitForReplicaSet(CLUSTER_REACHABLE_TIMEOUT_MS);
            mongoControl.applyServerParameters(CLIENT_MODE);
            mongoControl.startOccupancyPolling();
            transport = new MongoTransport(new MongoTransport.Settings(SERVER_HOSTS, config.mongodb.port,
                    config.mongodb.replicaSet, config.mongodb.database, config.mongodb.collection,
                    config.mongodb.maxPoolSize, config.mongodb.serverSelectionTimeoutMs,
                    config.mongodb.poolWaitTimeoutMs, siteBindHosts, CLIENT_SITE_NAMES,
                    config.mongodbServerHooks()));
        } else {
            connectAdminStubs();
            waitForClusterReachable();
            applyNodeFailureConfig();
            transport = new KvFramedTransport(SERVER_HOSTS, CLIENT_BASE_PORT,
                    INGRESS_CONNECTIONS_PER_SERVER, siteBindHosts);
        }
        appSessions = new KvSessionClient[NUM_APPLICATIONS][SESSIONS_PER_APPLICATION];
        for (int app = 0; app < NUM_APPLICATIONS; app++) {
            for (int session = 0; session < SESSIONS_PER_APPLICATION; session++) {
                int siteId = session % numSites;
                // The session's index among its application's sessions at this site, so every
                // application spreads evenly over a site's driver clients (KvTransport.forSession).
                appSessions[app][session] = new KvSessionClient(app + 1, siteId,
                        CLIENT_SITE_NAMES.get(siteId), transport.forSession(session / numSites), CLIENT_MODE,
                        RTT_WINDOW_SIZE, CLIENT_RETRY_LIMIT, CLIENT_LOST_TIMEOUT_MS,
                        readSlasByApp.get(app + 1), writeSlasByApp.get(app + 1),
                        EXPLORATION_FRACTION, FOLLOWER_LIN_READS,
                        ADMISSION_AWARE_ROUTING, ADMIT_RATE_GAMMA,
                        // Distinct deterministic seed per (application, session).
                        SEED + ((long) (app + 1) << 32) + session);
            }
        }
        System.out.println("Started " + NUM_APPLICATIONS + " applications x " + SESSIONS_PER_APPLICATION
                + " sessions (" + (NUM_APPLICATIONS * SESSIONS_PER_APPLICATION) + " session clients) across "
                + numSites + " client sites " + CLIENT_SITE_NAMES
                + (GEO_ENABLED ? " (per-site source IPs bound)" : " (geo disabled; no source binding)"));

        Thread coordinator = startPhasedInjection();
        // The coordinator thread ends the run: final report, cluster shutdown,
        // System.exit with the violation-derived exit code.
        coordinator.join();
    }

    // ===== Cluster admin (over gRPC; the servers are separate processes) =====

    private static void connectAdminStubs() {
        adminStubs = new AdminGrpc.AdminBlockingStub[NUM_OF_SERVERS];
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(SERVER_HOSTS.get(i), SERVER_BASE_PORT + i + 1)
                    .usePlaintext().build();
            adminChannels.add(channel);
            adminStubs[i] = AdminGrpc.newBlockingStub(channel);
        }
    }

    private static AdminStatusReply statusOf(int nodeId) throws StatusRuntimeException {
        return adminStubs[nodeId]
                .withDeadlineAfter(ADMIN_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS)
                .getStatus(AdminStatusRequest.getDefaultInstance());
    }

    /** Block until every server process answers its admin status probe. */
    private static void waitForClusterReachable() {
        System.out.printf("Waiting for %d server processes...%n", NUM_OF_SERVERS);
        Set<Integer> reachable = new HashSet<>();
        long deadline = System.currentTimeMillis() + CLUSTER_REACHABLE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline && reachable.size() < NUM_OF_SERVERS) {
            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (reachable.contains(i)) {
                    continue;
                }
                try {
                    statusOf(i);
                    reachable.add(i);
                } catch (StatusRuntimeException e) {
                    // Not up yet; retried until the deadline.
                }
            }
            if (reachable.size() < NUM_OF_SERVERS) {
                sleepQuietly(200);
            }
        }
        if (reachable.size() < NUM_OF_SERVERS) {
            List<Integer> missing = new ArrayList<>();
            for (int i = 0; i < NUM_OF_SERVERS; i++) {
                if (!reachable.contains(i)) {
                    missing.add(i);
                }
            }
            System.err.println("Server processes unreachable after "
                    + CLUSTER_REACHABLE_TIMEOUT_MS + "ms: " + missing
                    + " (are the ServerNode processes running? check server_<id>.log)");
            System.exit(1);
        }
        System.out.println("All server processes reachable.");
    }

    /** The id of the current healthy leader, or -1 if none is visible. */
    private static int findLeaderId() {
        if (mongoControl != null) {
            return mongoControl.findLeaderId();
        }
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            try {
                AdminStatusReply status = statusOf(i);
                if ("LEADER".equals(status.getRole()) && !status.getTrafficDropped()) {
                    return i;
                }
            } catch (StatusRuntimeException e) {
                // Unreachable node cannot be the healthy leader; keep looking.
            }
        }
        return -1;
    }

    private static int waitForLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leader = findLeaderId();
            if (leader >= 0) {
                return leader;
            }
            sleepQuietly(50);
        }
        return -1;
    }

    private static void setDropTraffic(int nodeId, boolean drop) {
        if (mongoControl != null) {
            throw new UnsupportedOperationException(
                    "failure injection is not supported with transport: mongodb (the config loader rejects it)");
        }
        adminStubs[nodeId]
                .withDeadlineAfter(ADMIN_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS)
                .setDropTraffic(SetDropTrafficRequest.newBuilder().setDrop(drop).build());
    }

    /** Ask every server process to shut down; failures are reported, not fatal. */
    private static void shutdownCluster() {
        if (mongoControl != null) {
            mongoControl.shutdownCluster();
            return;
        }
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            try {
                adminStubs[i]
                        .withDeadlineAfter(ADMIN_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS)
                        .shutdown(ShutdownRequest.getDefaultInstance());
            } catch (StatusRuntimeException e) {
                System.err.printf("Shutdown request to server %d failed: %s%n", i, e.getStatus());
            }
        }
        for (ManagedChannel channel : adminChannels) {
            channel.shutdown();
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ===== Failure injection =====

    private static void applyNodeFailureConfig() {
        if (!ENABLE_NODE_NETWORK_FAILURE) {
            return;
        }
        if (FAILED_NODE_ID < 0 || FAILED_NODE_ID >= NUM_OF_SERVERS) {
            System.err.printf("Invalid FAILED_NODE_ID=%d (valid range: 0 to %d)%n",
                    FAILED_NODE_ID, Math.max(0, NUM_OF_SERVERS - 1));
            return;
        }
        setDropTraffic(FAILED_NODE_ID, true);
        System.out.printf("Simulated node failure enabled on server %d (inter-server RPCs are dropped)%n",
                FAILED_NODE_ID);
    }

    /**
     * Run the failure schedule (config failureSchedule) against the workload clock: each step
     * is resolved when it falls due, a failure against the leader the cluster has at that
     * moment, a recovery against the node failed longest, and applied through the admin RPC
     * that makes a node drop all inter-server traffic and fail every client request. Events
     * print into run.log with the workload second so the analysis can mark them.
     */
    private static void scheduleFailurePlan() {
        if (FAILURE_PLAN.nextStep() == null) {
            return;
        }
        Thread failureScheduler = new Thread(() -> {
            while (experimentRunning && FAILURE_PLAN.nextStep() != null && !Thread.currentThread().isInterrupted()) {
                long dueAtMs = experimentStartTime + FAILURE_PLAN.nextStep().atSeconds() * 1000L;
                if (System.currentTimeMillis() >= dueAtMs) {
                    applyNextFailureStep();
                    continue;
                }
                try {
                    Thread.sleep(Math.min(50, Math.max(1, dueAtMs - System.currentTimeMillis())));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "FailureScheduler");
        failureScheduler.setDaemon(true);
        failureScheduler.start();
    }

    private static void applyNextFailureStep() {
        FailurePlan.Step step = FAILURE_PLAN.nextStep();
        long elapsedSeconds = (System.currentTimeMillis() - experimentStartTime) / 1000;
        // A failure needs the healthy leader to resolve against (findLeaderId ignores failed
        // nodes); wait through an election if one is in progress.
        int leaderId = step.action() == FailurePlan.Action.FAIL ? waitForLeader(LEADER_ELECTION_TIMEOUT_MS) : -1;
        FailurePlan.Resolved resolved;
        try {
            resolved = FAILURE_PLAN.resolve(leaderId);
        } catch (IllegalStateException e) {
            System.err.printf("[%ds] Failure schedule abandoned at step %s: %s%n", elapsedSeconds, step, e.getMessage());
            FAILURE_PLAN.abandon();
            return;
        }
        boolean drop = resolved.step().action() == FailurePlan.Action.FAIL;
        try {
            setDropTraffic(resolved.nodeId(), drop);
        } catch (StatusRuntimeException e) {
            System.err.printf("[%ds] Failure schedule abandoned: %s of node %d failed: %s%n", elapsedSeconds,
                    drop ? "failure" : "recovery", resolved.nodeId(), e.getStatus());
            FAILURE_PLAN.abandon();
            return;
        }
        if (drop) {
            System.out.printf("[%ds] Failure schedule: failed node %d (%s at %d s; leader was %d)%n", elapsedSeconds,
                    resolved.nodeId(), resolved.step().target(), resolved.step().atSeconds(), leaderId);
        } else {
            System.out.printf("[%ds] Failure schedule: recovered node %d (failed longest; at %d s)%n", elapsedSeconds,
                    resolved.nodeId(), resolved.step().atSeconds());
        }
    }

    // ===== Phased workload =====

    private static Thread startPhasedInjection() {
        System.out.println("Waiting for leader election...");
        int leaderId = waitForLeader(LEADER_ELECTION_TIMEOUT_MS);
        if (leaderId < 0) {
            System.err.println("No leader elected, aborting injection");
            System.exit(1);
        }
        System.out.println("Leader elected: server " + leaderId);

        System.out.println("Starting phased workload experiment...");
        printPhaseConfig();

        experimentStartTime = System.currentTimeMillis();
        experimentRunning = true;
        selectAndStartNewPhase();
        scheduleFailurePlan();

        Thread phaseManager = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - experimentStartTime >= TOTAL_EXPERIMENT_DURATION_MS) {
                        experimentRunning = false;
                        break;
                    }
                    if (currentTime >= phaseEndTime && experimentRunning) {
                        Thread.sleep(500);
                        selectAndStartNewPhase();
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "PhaseManager");
        phaseManager.setDaemon(true);
        phaseManager.start();

        // The injection workers share nothing but the phase schedule and the
        // sent counter: sessions are sharded across them, so every session
        // keeps a single driving thread.
        LongAdder totalInjected = new LongAdder();
        // Requests the schedule called for that no thread ever sent. A worker
        // that falls a full tick behind re-anchors instead of catching up, and
        // that dropped budget is invisible in every other counter: the run then
        // reports a smaller offered load as though it were the configured one,
        // which silently turns an overload phase into a milder one.
        LongAdder skippedBudget = new LongAdder();
        // Skips during the opening phase are excluded from the warning below.
        // That phase exists to absorb JIT warm-up and always loses a little: the
        // two smoke runs skipped 98,333 and 14,000 requests there and exactly
        // zero afterwards. A warning that fires on every healthy run is noise.
        LongAdder skippedWarmUp = new LongAdder();
        Thread[] workers = new Thread[INJECTOR_THREADS];
        for (int t = 0; t < INJECTOR_THREADS; t++) {
            int shard = t;
            workers[t] = new Thread(
                    () -> injectShard(shard, totalInjected, skippedBudget, skippedWarmUp),
                    "SystemInjector-" + shard);
            workers[t].start();
        }

        // The coordinator reports progress while the workers run, then ends
        // the run: final report, cluster shutdown, violation-derived exit code.
        Thread coordinator = new Thread(() -> {
            long lastReportTime = System.currentTimeMillis();
            // Progress lines report per-interval deltas, not running totals.
            long lastSent = 0;
            long lastSkipped = 0;
            long lastServed = 0;
            long lastRejected = 0;
            long lastLost = 0;
            long lastViolations = 0;
            double lastPredicted = 0;
            double lastRealized = 0;

            while (!Thread.currentThread().isInterrupted() && experimentRunning) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastReportTime >= 5000) {
                    Phase phase = currentPhase;
                    long elapsedSeconds = (currentTime - experimentStartTime) / 1000;
                    long sent = totalInjected.sum();
                    long served = ClientMetricsTracker.totalResponses();
                    long rejectedNow = ClientMetricsTracker.totalRejected();
                    long lostNow = ClientMetricsTracker.totalLost();
                    long violationsNow = ClientMetricsTracker.totalViolations();
                    double predictedNow = ClientMetricsTracker.totalPredictedProfit();
                    double realizedNow = ClientMetricsTracker.totalRealizedProfit();
                    long skippedNow = skippedBudget.sum();
                    System.out.printf(
                            "[%02ds] Sent=%d | Skipped=%d | Served=%d | Rejected=%d | Lost=%d | Violations=%d | PredictedProfit=%.0f | RealizedProfit=%.0f | Phase=%s | TotalTPS=%d%n",
                            elapsedSeconds, sent - lastSent, skippedNow - lastSkipped,
                            served - lastServed,
                            rejectedNow - lastRejected, lostNow - lastLost,
                            violationsNow - lastViolations,
                            predictedNow - lastPredicted, realizedNow - lastRealized,
                            phase.name, phase.totalTPS);
                    lastSkipped = skippedNow;
                    lastSent = sent;
                    lastServed = served;
                    lastRejected = rejectedNow;
                    lastLost = lostNow;
                    lastViolations = violationsNow;
                    lastPredicted = predictedNow;
                    lastRealized = realizedNow;
                    lastReportTime = currentTime;
                }
            }

            for (Thread worker : workers) {
                try {
                    worker.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Give in-flight requests a moment to resolve, then report.
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            ClientMetricsTracker.flushNow();
            long violations = ClientMetricsTracker.totalViolations();
            long sentTotal = totalInjected.sum();
            long skippedTotal = skippedBudget.sum();
            System.out.printf(
                    "%nExperiment completed. Sent=%d Skipped=%d Responses=%d Rejected=%d Lost=%d Violations=%d"
                            + " ClientDropped=%d%n",
                    sentTotal, skippedTotal, ClientMetricsTracker.totalResponses(),
                    ClientMetricsTracker.totalRejected(),
                    ClientMetricsTracker.totalLost(), violations, ClientMetricsTracker.totalClientDropped());
            // 0.01% is a handful of requests lost to a JIT pause at startup and
            // says nothing; warning on it would train the reader to ignore the
            // warning that matters. The count is reported unconditionally above.
            long skippedMeasured = skippedTotal - skippedWarmUp.sum();
            long configured = sentTotal + skippedTotal;
            if (skippedWarmUp.sum() > 0) {
                System.out.printf("  of which %d were skipped during %s and are not counted below%n",
                        skippedWarmUp.sum(), PHASES.get(0).name);
            }
            if (skippedMeasured * 10000L > configured) {
                System.out.printf(
                        "WARNING: the injector skipped %d requests outside %s, of %d the phase schedule"
                                + " called for (%.2f%%). The measured offered rate is NOT the configured one,"
                                + " so any phase defined as a multiple of C is running at an unknown"
                                + " multiple. Raise workload.injectorThreads or lower the phase rates.%n",
                        skippedMeasured, PHASES.get(0).name, configured,
                        100.0 * skippedMeasured / configured);
            }
            if (transport instanceof MongoTransport mongoTransport) {
                System.out.print(mongoTransport.driverEventSummary());
            }
            var errorsByCause = ClientMetricsTracker.transportErrorsByCause();
            if (!errorsByCause.isEmpty()) {
                System.out.println("Transport errors by cause (every attempt, retries included):");
                for (var e : errorsByCause) {
                    System.out.printf("  %10d  %s%n", e.getValue(), e.getKey());
                    for (String sample : ClientMetricsTracker.transportErrorSamples(e.getKey())) {
                        System.out.println("              sample: " + sample);
                    }
                }
            }
            for (KvSessionClient[] sessions : appSessions) {
                for (KvSessionClient client : sessions) {
                    client.close();
                }
            }
            transport.close();
            if (mongoControl != null) {
                // No serverStatus poll may be in flight while the nodes go down.
                mongoControl.stopOccupancyPolling();
            }
            System.out.println("Shutting down server processes...");
            shutdownCluster();
            if (mongoControl != null) {
                mongoControl.close();
            }
            // Deadline violations are an expected experiment outcome under
            // overload, not a process correctness failure.
            System.exit(0);
        }, "InjectionCoordinator");
        coordinator.start();
        return coordinator;
    }

    /**
     * One injection worker. It owns sessions shard, shard + K, ... of every
     * application (K = injectorThreads) and injects its slice of each 100ms
     * tick's request budget, with the division remainder spread over the
     * lowest shards so the per-tick total stays exact.
     */
    private static void injectShard(int shard, LongAdder totalInjected,
            LongAdder skippedBudget, LongAdder skippedWarmUp) {
        // Distinct deterministic stream per shard; Random scrambles the seed
        // internally, so sequential seeds do not correlate the streams.
        Random random = new Random(SEED * 1_000_003L + shard);
        KvSessionClient[][] ownSessions = new KvSessionClient[NUM_APPLICATIONS][];
        for (int app = 0; app < NUM_APPLICATIONS; app++) {
            int count = (SESSIONS_PER_APPLICATION - shard + INJECTOR_THREADS - 1) / INJECTOR_THREADS;
            ownSessions[app] = new KvSessionClient[count];
            int j = 0;
            for (int s = shard; s < SESSIONS_PER_APPLICATION; s += INJECTOR_THREADS) {
                ownSessions[app][j++] = appSessions[app][s];
            }
        }
        long localSeq = 0;

        // Ticks are anchored to an absolute schedule (nextTickMs += TICK_MS)
        // so sleep overshoot cannot accumulate into rate loss: a wake that
        // lands 1 ms late sleeps 1 ms less before the next tick, keeping the
        // average period exactly TICK_MS. A genuine stall (more than one full
        // period behind) re-anchors instead of catching up: the missed budget
        // is dropped deliberately, because injecting it late would burst-load
        // the system at exactly the moments it is already struggling (the
        // offered process stays open loop, with no compensating bursts).
        long nextTickMs = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted() && experimentRunning) {
            try {
                Phase phase = currentPhase;
                int perTick = Math.max(1, phase.totalTPS * TICK_MS / 1000);
                int share = perTick / INJECTOR_THREADS + (shard < perTick % INJECTOR_THREADS ? 1 : 0);

                // The workload no longer picks consistency levels: each
                // request is drawn from the phase's SLA mix, which fixes
                // the application, read vs write, and the SLA it names;
                // the decision policy chooses the rung. Keys come from
                // the configured distribution (uniform or zipfian), the
                // same for reads and writes.
                long now = System.currentTimeMillis();
                for (int i = 0; i < share; i++) {
                    MixEntry entry = phase.sample(random);
                    KvSessionClient[] pool = ownSessions[entry.applicationId - 1];
                    KvSessionClient client = pool.length == 1 ? pool[0] : pool[random.nextInt(pool.length)];
                    String key = "user" + keySampler.next(random);
                    if (entry.isRead) {
                        client.sendRead(key, entry.slaId);
                    } else {
                        client.sendWrite(key, "v-" + now + "-" + shard + "-" + localSeq, entry.slaId);
                    }
                    localSeq++;
                    totalInjected.increment();
                }

                nextTickMs += TICK_MS;
                long sleepTime = nextTickMs - System.currentTimeMillis();
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                } else if (-sleepTime >= TICK_MS) {
                    // Stalled a full period or more: drop the missed ticks, and
                    // record what they would have carried. Dropping is right -
                    // injecting late would burst-load a system already
                    // struggling - but it has to be counted, or the offered rate
                    // in the results is not the offered rate in the config.
                    long missed = (-sleepTime / TICK_MS) * (long) share;
                    skippedBudget.add(missed);
                    if (phase == PHASES.get(0)) {
                        skippedWarmUp.add(missed);
                    }
                    nextTickMs = System.currentTimeMillis();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("System injection error: " + e.getMessage());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void selectAndStartNewPhase() {
        if (!experimentRunning) {
            return;
        }
        Phase newPhase = selectNextPhase();
        if (newPhase == null) {
            System.out.println("\nAll phases completed. Stopping experiment.");
            experimentRunning = false;
            return;
        }
        long newEndTime = System.currentTimeMillis() + (newPhase.durationSeconds * 1000L);
        long experimentEndTime = experimentStartTime + TOTAL_EXPERIMENT_DURATION_MS;
        if (newEndTime > experimentEndTime) {
            newEndTime = experimentEndTime;
        }
        synchronized (phaseLock) {
            currentPhase = newPhase;
            phaseEndTime = newEndTime;
        }
        System.out.println("\n========================================");
        System.out.printf("Starting Phase %d/%d: %s%n", currentPhaseIndex.get(), PHASES.size(), newPhase.name);
        System.out.printf("   Duration: %ds | Total TPS: %d | Read share: %.0f%%%n",
                newPhase.durationSeconds, newPhase.totalTPS, newPhase.readShare() * 100.0);
        System.out.println("   Mix '" + newPhase.mixName + "': " + describeMix(newPhase));
        System.out.println("========================================");
    }

    private static Phase selectNextPhase() {
        int index = currentPhaseIndex.getAndIncrement();
        if (index >= PHASES.size()) {
            return null;
        }
        return PHASES.get(index);
    }

    private static void printPhaseConfig() {
        System.out.println("\nExperiment Configuration:");
        System.out.printf("   Total Duration: %d seconds%n", TOTAL_EXPERIMENT_DURATION_MS / 1000);
        for (Phase p : PHASES) {
            System.out.printf("   %d. %s: %ds | TotalTPS=%d | ReadShare=%.0f%% | Mix=%s%n",
                    PHASES.indexOf(p) + 1, p.name, p.durationSeconds, p.totalTPS,
                    p.readShare() * 100.0, p.mixName);
        }
        System.out.println();
    }

    /** "app1/read/sla1=30%, app1/write/sla1=3%, ..." with weights normalized. */
    private static String describeMix(Phase phase) {
        double total = phase.mix.stream().mapToDouble(e -> e.weight).sum();
        StringBuilder sb = new StringBuilder();
        for (MixEntry entry : phase.mix) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("app").append(entry.applicationId)
                    .append(entry.isRead ? "/read/sla" : "/write/sla").append(entry.slaId)
                    .append(String.format("=%.1f%%", entry.weight / total * 100.0));
        }
        return sb.toString();
    }

    /** Clear this process's result CSV at startup; each server process clears its own. */
    private static void clearCSVFiles() {
        File file = new File("client_metrics_global.csv");
        if (file.exists() && !file.delete()) {
            System.out.println("Warning: could not delete client_metrics_global.csv");
        }
    }
}
