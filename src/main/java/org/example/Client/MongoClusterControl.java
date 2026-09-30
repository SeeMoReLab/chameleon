package org.example.Client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.example.Utility.ExperimentConfig;
import org.example.raft.ReadLevel;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;

/**
 * Cluster administration for the MongoDB transport (MONGODB_PORT.md,
 * Milestone 3), the counterpart of the WorkloadDriver's admin gRPC against
 * the Java store: reachability and leader discovery through hello, pushing
 * the Chameleon server parameters and the SLA table through setParameter,
 * polling the chameleon serverStatus section into occupancy_<n>.csv in the
 * schema the analysis scripts read, and shutting the nodes down.
 *
 * One direct connection per node so each command lands on the node it names,
 * independent of the replica set's view of itself.
 */
public final class MongoClusterControl implements AutoCloseable {

    private static final int ADMIN_TIMEOUT_MS = 2000;

    private final ExperimentConfig config;
    private final MongoClient[] nodeClients;
    private final List<String> hosts;
    private volatile Thread poller;
    private volatile boolean polling;

    public MongoClusterControl(ExperimentConfig config) {
        this.config = config;
        this.hosts = List.copyOf(config.cluster.serverHosts);
        this.nodeClients = new MongoClient[hosts.size()];
        for (int node = 0; node < hosts.size(); node++) {
            ServerAddress address = new ServerAddress(hosts.get(node), config.mongodb.port);
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyToClusterSettings(b -> b.hosts(List.of(address))
                            .mode(ClusterConnectionMode.SINGLE)
                            .serverSelectionTimeout(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                    .applyToSocketSettings(b -> b.connectTimeout(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                            .readTimeout(ADMIN_TIMEOUT_MS * 5, TimeUnit.MILLISECONDS))
                    .build();
            nodeClients[node] = MongoClients.create(settings);
        }
    }

    public int numNodes() {
        return nodeClients.length;
    }

    private Document admin(int node, Document command) {
        return nodeClients[node].getDatabase("admin").runCommand(command);
    }

    /** The node's hello reply, or null when it does not answer. */
    private Document helloOrNull(int node) {
        try {
            return admin(node, new Document("hello", 1));
        } catch (MongoException e) {
            return null;
        }
    }

    /**
     * Block until every node answers hello as a primary or secondary of the
     * configured replica set and exactly one primary is visible. Fails
     * explicitly on timeout with the list of nodes that were not ready.
     */
    public void waitForReplicaSet(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Set<Integer> ready = new HashSet<>();
        int primaries = 0;
        while (System.currentTimeMillis() < deadline) {
            ready.clear();
            primaries = 0;
            for (int node = 0; node < nodeClients.length; node++) {
                Document hello = helloOrNull(node);
                if (hello == null) {
                    continue;
                }
                if (!config.mongodb.replicaSet.equals(hello.getString("setName"))) {
                    continue;
                }
                boolean primary = Boolean.TRUE.equals(hello.getBoolean("isWritablePrimary"));
                boolean secondary = Boolean.TRUE.equals(hello.getBoolean("secondary"));
                if (primary) {
                    primaries++;
                }
                if (primary || secondary) {
                    ready.add(node);
                }
            }
            if (ready.size() == nodeClients.length && primaries == 1) {
                System.out.printf("Replica set %s ready: %d members, primary is node %d%n",
                        config.mongodb.replicaSet, ready.size(), findLeaderId());
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        List<Integer> missing = new ArrayList<>();
        for (int node = 0; node < nodeClients.length; node++) {
            if (!ready.contains(node)) {
                missing.add(node);
            }
        }
        throw new IllegalStateException("replica set " + config.mongodb.replicaSet + " not ready after " + timeoutMs
                + " ms: nodes not primary/secondary " + missing + ", primaries visible " + primaries);
    }

    /** The node id of the current primary, or -1 when none answers as primary. */
    public int findLeaderId() {
        for (int node = 0; node < nodeClients.length; node++) {
            Document hello = helloOrNull(node);
            if (hello != null && Boolean.TRUE.equals(hello.getBoolean("isWritablePrimary"))) {
                return node;
            }
        }
        return -1;
    }

    // ===== Server parameters =====

    /** The chameleonSlaTable document: strength is the ReadLevel ordinal for reads, the concern for writes. */
    public static Document slaTableDocument(ExperimentConfig config) {
        List<Document> read = new ArrayList<>();
        List<Document> write = new ArrayList<>();
        for (ExperimentConfig.AppSlas app : config.slas) {
            for (ExperimentConfig.Sla sla : app.read) {
                List<Document> rungs = new ArrayList<>();
                for (ExperimentConfig.SlaRung rung : sla.rungs) {
                    rungs.add(new Document("strength", ReadLevel.valueOf(rung.level).getNumber())
                            .append("latencyMs", rung.latencyMs).append("profit", rung.profit));
                }
                read.add(new Document("app", app.applicationId).append("sla", sla.slaId).append("rungs", rungs));
            }
            for (ExperimentConfig.Sla sla : app.write) {
                List<Document> rungs = new ArrayList<>();
                for (ExperimentConfig.SlaRung rung : sla.rungs) {
                    rungs.add(new Document("strength", rung.concern)
                            .append("latencyMs", rung.latencyMs).append("profit", rung.profit));
                }
                write.add(new Document("app", app.applicationId).append("sla", sla.slaId).append("rungs", rungs));
            }
        }
        return new Document("read", read).append("write", write);
    }

    /**
     * The server parameters a run pushes, in push order. Every knob the two stores share is
     * here so a mongod run and a Java run differ only in the store: the hard cap
     * (server.ingressHardCapInFlight) and the price ceiling (chameleon.lambdaMax) included.
     * The ceiling goes before the floor: the parameters land one at a time, ExperimentConfig
     * has already required lambdaMax > lambdaMin, and a new ceiling is consistent with the old
     * floor whenever it is consistent with the new one, so the pair is never inverted in
     * between.
     */
    public static Document serverParameters(ExperimentConfig config, ClientMode mode) {
        return new Document("chameleonEnabled", true)
                .append("chameleonServerDecision", mode.chameleonDecision())
                .append("chameleonSMax", config.server.sMax)
                .append("chameleonHardCapInFlight", config.server.ingressHardCapInFlight)
                .append("chameleonReplicationBudgetPerSecond", config.server.replicationBudgetPerSecond)
                .append("chameleonMaxWaitMs", config.server.maxWaitMs)
                .append("chameleonControlIntervalMs", config.chameleon.controlIntervalMs)
                .append("chameleonUTarget", config.chameleon.uTarget)
                .append("chameleonEta", config.chameleon.eta)
                .append("chameleonLambdaMax", config.chameleon.lambdaMax)
                .append("chameleonLambdaMin", config.chameleon.lambdaMin)
                .append("chameleonHistogramDecay", config.server.histogramDecay)
                .append("chameleonSlaTable", slaTableDocument(config));
    }

    /** Every Chameleon parameter the config expresses, pushed to every node; fails on the first refusal. */
    public void applyServerParameters(ClientMode mode) {
        if (!config.mongodbServerHooks()) {
            // Nothing is pushed, not even chameleonEnabled=false: the patched mongod starts with
            // the parameter at that default, and an unpatched mongod would reject the name.
            System.out.println("Chameleon server hooks disabled by config (mongodb.serverHooks: false);"
                    + " no parameters pushed to " + nodeClients.length + " nodes");
            return;
        }
        Document parameters = serverParameters(config, mode);
        for (int node = 0; node < nodeClients.length; node++) {
            for (String name : parameters.keySet()) {
                Document command = new Document("setParameter", 1).append(name, parameters.get(name));
                try {
                    admin(node, command);
                } catch (MongoException e) {
                    throw new IllegalStateException("setParameter " + name + " refused by node " + node + " ("
                            + hosts.get(node) + "): " + e.getMessage()
                            + " (is this the patched mongod with the chameleon module?)", e);
                }
            }
        }
        System.out.println("Chameleon parameters applied to " + nodeClients.length + " nodes (serverDecision="
                + mode.chameleonDecision() + ")");
    }

    // ===== Occupancy polling =====

    private static String roleLabel(String memberState) {
        if (memberState == null) {
            return "UNKNOWN";
        }
        return switch (memberState) {
            case "PRIMARY" -> "LEADER";
            case "SECONDARY" -> "FOLLOWER";
            default -> memberState;
        };
    }

    /**
     * Poll each node's chameleon serverStatus section and append one row per
     * poll to occupancy_<n>.csv in the Java store's schema. CrossCheckU has no
     * MongoDB counterpart and is written as nan.
     */
    public void startOccupancyPolling() {
        polling = true;
        poller = new Thread(() -> {
            long interval = config.mongodb.statusPollIntervalMs;
            long next = System.currentTimeMillis();
            while (polling) {
                for (int node = 0; node < nodeClients.length; node++) {
                    Document status;
                    try {
                        status = admin(node, new Document("serverStatus", 1));
                    } catch (MongoException e) {
                        continue;
                    }
                    Document section = status.get("chameleon", Document.class);
                    try {
                        if (!config.mongodbServerHooks()) {
                            // The patched binary still reports its section, but with the hooks idle it
                            // counts nothing; the stock fields are the only load signal.
                            appendOccupancyRow(node, stockOccupancyRow(status));
                        } else if (section != null) {
                            appendOccupancyRow(node, occupancyRow(section));
                        } else {
                            throw new IllegalStateException("serverStatus has no chameleon section although"
                                    + " mongodb.serverHooks is true");
                        }
                    } catch (RuntimeException e) {
                        // A dead poller thread would silently leave occupancy_<n>.csv empty; say so and stop.
                        System.err.println("occupancy polling stopped: node " + node + " (" + hosts.get(node) + "): " + e);
                        polling = false;
                        return;
                    }
                }
                next += interval;
                long sleep = next - System.currentTimeMillis();
                if (sleep > 0) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "MongoOccupancyPoller");
        poller.setDaemon(true);
        poller.start();
    }

    /** One occupancy CSV row from the patched server's chameleon serverStatus section. */
    private static String occupancyRow(Document section) {
        return String.format("%.6f,nan,%.3f,%d,%.8f,%s",
                section.getDouble("u"), section.getDouble("avgInFlight"),
                section.getInteger("inFlightAtClose"), section.getDouble("lambda"),
                roleLabel(section.getString("role")));
    }

    /**
     * One occupancy CSV row from stock serverStatus fields (mongodb.serverHooks: false):
     * no utilization or price, in-flight from the global lock's active client count
     * (operations currently holding the lock), role from the repl section.
     */
    static String stockOccupancyRow(Document status) {
        Document globalLock = status.get("globalLock", Document.class);
        Document activeClients = globalLock == null ? null : globalLock.get("activeClients", Document.class);
        // int32 in 8.x releases, int64 on master: read it as a Number.
        Object total = activeClients == null ? null : activeClients.get("total");
        if (!(total instanceof Number)) {
            throw new IllegalStateException("serverStatus lacks a numeric globalLock.activeClients.total");
        }
        long active = ((Number) total).longValue();
        Document repl = status.get("repl", Document.class);
        String role;
        if (repl == null) {
            role = "UNKNOWN";
        } else if (Boolean.TRUE.equals(repl.getBoolean("isWritablePrimary")) || Boolean.TRUE.equals(repl.getBoolean("ismaster"))) {
            role = "LEADER";
        } else if (Boolean.TRUE.equals(repl.getBoolean("secondary"))) {
            role = "FOLLOWER";
        } else {
            role = "UNKNOWN";
        }
        return String.format("nan,nan,%d.000,%d,nan,%s", active, active, role);
    }

    private static void appendOccupancyRow(int node, String row) {
        String csvPath = "occupancy_" + node + ".csv";
        File file = new File(csvPath);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                out.println("Timestamp,U,CrossCheckU,AvgInFlight,InFlightAtClose,Lambda,Role");
            }
            out.printf("%d,%s%n", System.currentTimeMillis(), row);
        } catch (IOException e) {
            System.err.println("Failed to write " + csvPath + ": " + e.getMessage());
        }
    }

    /** Stop the poller and wait for it, so no serverStatus is in flight when the clients close. */
    public void stopOccupancyPolling() {
        polling = false;
        Thread t = poller;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            poller = null;
        }
    }

    // ===== Shutdown =====

    /** Ask every node to shut down; failures are reported, not fatal (the connection drops mid-reply). */
    public void shutdownCluster() {
        for (int node = 0; node < nodeClients.length; node++) {
            try {
                admin(node, new Document("shutdown", 1).append("force", true));
            } catch (MongoException e) {
                if (!(e.getMessage() != null && e.getMessage().contains("closed"))) {
                    System.err.printf("Shutdown request to node %d reported: %s%n", node, e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        stopOccupancyPolling();
        for (MongoClient client : nodeClients) {
            client.close();
        }
    }
}
