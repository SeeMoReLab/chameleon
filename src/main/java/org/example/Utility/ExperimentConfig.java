package org.example.Utility;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * Experiment configuration, loaded from a JSON file at startup.
 *
 * Loading is strict by design: a missing file, malformed JSON, an unknown key
 * (usually a typo), or an invalid value all fail fast with an explicit error
 * instead of silently falling back to defaults.
 */
public final class ExperimentConfig {

    // --- Schema (wrapper types so that missing keys are detectable as null) ---

    public String mode;
    public Long seed; // base seed for every PRNG (workload mix/keys, routing exploration, election jitter)
    public Cluster cluster;
    public ServerConfig server;
    public Chameleon chameleon;
    public ClientConfig client;
    public Workload workload;
    public List<AppSlas> slas;
    public NodeFailure nodeFailure;
    public TimedFailure timedFailure;
    public Geo geo;
    // Optional (MONGODB_PORT.md): "framed" (default, the Java KV store) or "mongodb" (the patched
    // mongod replica set). With "mongodb" the mongodb block below is required.
    public String transport;
    public Mongodb mongodb;

    /** MongoDB transport settings; only read when transport is "mongodb". */
    public static final class Mongodb {
        public Integer port;                     // every mongod listens on this port on its own cluster.serverHosts address
        public String replicaSet;                // replica set name passed to --replSet and rs.initiate
        public String database;                  // database holding the key-value collection
        public String collection;                // collection holding documents {_id: key, v: value}
        public Integer maxPoolSize;              // driver connections per client site (bounds in-flight ops per site)
        public Integer serverSelectionTimeoutMs; // how long the driver waits for a node before the request fails
        public Integer poolWaitTimeoutMs;        // how long a checkout waits for a pooled connection (must cover one open, two round trips)
        public Integer statusPollIntervalMs;     // period of the serverStatus poll that writes occupancy_<n>.csv
        /**
         * Whether the server-side Chameleon module is used (default true). False leaves
         * chameleonEnabled at its server default of false, pushes no parameters, grades
         * from the driver's own reply metadata instead of the $chameleon envelope, and
         * records occupancy from stock serverStatus fields. Valid only with the
         * client-decided modes weakest and strongest; measures the hooks' cost and lets
         * the harness run against an unpatched mongod.
         */
        public Boolean serverHooks;
        /**
         * mongod binary the CloudLab runner starts on the node (default: the experiment binary,
         * ~/tuneable/mongod, built by build_mongod.sh). A leading ~ expands to the node's home.
         * Points a run at the unpatched control or a stock release; only meaningful together
         * with serverHooks: false, since those binaries reject the Chameleon parameters.
         */
        public String binaryPath;
    }

    public static final class Cluster {
        public Integer numServers;
        public List<String> serverHosts; // one entry per server
        public Integer serverBasePort;   // Raft/admin server i listens on basePort + i + 1
        public Integer clientBasePort;   // framed client ingress i listens on basePort + i + 1
        public Integer preferredLeaderId; // first-election cue (-1 = none): this node gets a shorter election timeout
    }

    /** Mode-independent server mechanics. */
    public static final class ServerConfig {
        public Integer maxWaitMs;          // clamp on every server-side wait; expiry falls back / acks what is there
        // Most requests the ingress event loop will hold before it refuses new
        // ones. Previously derived as 1.5 * sMax, which tied the backstop to the
        // price's scale: a calibration probe pins sMax far above reach so the
        // price cannot influence its own input, and that silently removed the
        // backstop too, letting the receive queue grow to twenty seconds deep
        // (MONGODB_BASELINES.md). Separating them lets a probe disable the price
        // and keep flow control. 1.5 * sMax remains the value to use in a config
        // that is not probing.
        public Integer ingressHardCapInFlight;
        public Double sMax;                // occupancy slot budget; the price's scale (the hard cap is ingressHardCapInFlight)
        public Double replicationBudgetPerSecond; // leader replication rate budget in entries/s (universal admission)
        public Integer maxEntriesPerReplicationBatch; // entries in one ordered replication-stream message
        public Integer maxInflightReplicationBatchesPerFollower; // bounded speculative pipeline window
        public Integer replicationFlowControlWindowBytes; // HTTP/2 flow-control window on the server-server path (both directions); must hold the inflight batch window in bytes
        // Smallest gap, in ms, between two sends to the same follower when the
        // batch is short of maxEntriesPerReplicationBatch. The pipeline is
        // otherwise self-clocked - a follower acknowledgement pumps the next
        // batch immediately - so batch size ends up proportional to round-trip
        // time and a low-latency topology batches far worse than a distant one.
        // A full batch never waits. 0 disables it (the self-clocked behaviour).
        public Double replicationLingerMs;
        public Boolean followerLinearizableReads; // followers serve LIN via a leader read-index round (all modes)
        public Double histogramDecay;      // service-time histogram decay per 100 ms refresh tick, in (0, 1)
    }

    /** Chameleon economics; consulted only by the chameleon* modes. */
    public static final class Chameleon {
        public Integer controlIntervalMs;  // utilization / price-controller interval
        public Double uTarget;             // price controller utilization target (~0.85)
        public Double eta;                 // price controller gain (~1)
        public Double lambdaMin;           // price floor; only its order of magnitude matters
        public Double lambdaMax;           // price ceiling; anti-windup, see validation
    }

    /** Mode-independent client mechanics. */
    public static final class ClientConfig {
        public Integer rttWindowSize;      // per-node sliding window of RTT samples (no-wait replies only)
        public Integer retryLimit;         // redirect/failure resends before giving up on a request
        public Integer lostTimeoutMs;      // no response within this -> the client scores the request as lost
        public Integer ingressConnectionsPerServer; // shared long-lived framed TCP connections to each server
        public Double explorationFraction; // fraction of reads routed randomly to keep per-node windows fresh (all routing options)
        public Boolean admissionAwareRouting; // route by decayed per-node admit/reject ratio (Pileus and lowest-RTT alike)
        public Double admitRateGamma;      // per-second decay of the admit/reject counters, in (0, 1)
    }

    /** Key selection for the workload; reads and writes draw from the same distribution. */
    public static final class Workload {
        public Integer keySpace;        // number of distinct keys
        public String keyDistribution;  // "uniform" or "zipfian"
        public Double zipfianExponent;  // skew; weight of rank r is 1/(r+1)^exponent (used when zipfian)
        public Integer sessionsPerApplication; // independent session clients per app, drawn uniformly per request
        public Integer injectorThreads; // injection threads; sessions are sharded across them (each session stays single-threaded)
        public Map<String, List<SlaShare>> mixes; // named weight vectors phases reference by key
        public List<PhaseConfig> phases;
    }

    /** SLAs registered per application; requests carry only (applicationId, slaId). */
    public static final class AppSlas {
        public Integer applicationId;
        public List<Sla> read;
        public List<Sla> write;
    }

    public static final class Sla {
        public Integer slaId;
        public List<SlaRung> rungs;
    }

    /**
     * One rung (kappa, delta, pi). Read rungs set level (a ReadLevel name),
     * write rungs set concern (1..majority); exactly one of the two.
     */
    public static final class SlaRung {
        public String level;
        public Integer concern;
        public Double latencyMs;
        public Double profit;
    }

    public static final class NodeFailure {
        public Boolean enabled;    // drop all inter-server RPCs on one node
        public Integer failedNodeId;
    }

    public static final class TimedFailure {
        public Boolean enabled;      // fail one node N seconds after start
        public String targetRole;    // LEADER or FOLLOWER
        public Integer afterSeconds;
    }

    /**
     * Simulated geo latency, applied by run_all_cloudlab.sh (Linux tc/netem),
     * not by the Java processes. The delay rules match on (source IP,
     * destination IP) pairs: the runner rewrites cluster.serverHosts to
     * 127.0.1.1..N, and client site k binds its connections to
     * {@link #clientSiteBindHost} (127.0.2.(k+1)). A local run_all.sh run
     * refuses geo-enabled configs outright.
     */
    public static final class Geo {
        public Boolean enabled;
        // interServerLatencyMs[i][j]: one-way latency in ms added from server
        // i to server j (numServers x numServers; the diagonal must be 0).
        public Double[][] interServerLatencyMs;
        // Client sites. Every session belongs to one site (sessions are spread
        // evenly: session s of every application lives at site s mod #sites),
        // and site k's requests ride connections bound to clientSiteBindHost(k)
        // so the delay rules can tell sites apart. Defined and used even when
        // geo is disabled (sites then differ only in name; nothing binds).
        public List<ClientSite> clientSites;
        // Optional: the source address each client site binds, one per
        // clientSites entry. Omitted for a single-host run, where the sites
        // share the machine and take the loopback convention below. A
        // distributed run (run_all_cloudlab.sh --hosts) sets this to routable
        // aliases on the client machine, since loopback addresses cannot be
        // reached from the server machines.
        public List<String> clientSiteBindHosts;
    }

    /** One client site: a name for the ledger and its one-way delay to each server. */
    public static final class ClientSite {
        public String name;
        // latencyMs[j]: one-way latency in ms added between this site and
        // server j (numServers entries).
        public Double[] latencyMs;
    }

    /**
     * The loopback IP client site {@code siteIndex}'s connections bind as
     * their source when geo is enabled. Fixed convention shared with
     * run_all_cloudlab.sh (via the key=value printer below): servers own
     * 127.0.1.x, client sites own 127.0.2.x, and 127.0.0.1 stays the
     * orchestration address (admin RPCs), which no delay rule matches.
     */
    public static String clientSiteBindHost(int siteIndex) {
        if (siteIndex < 0 || siteIndex > 253) {
            throw new IllegalArgumentException("client site index must be in [0, 253], got " + siteIndex);
        }
        return "127.0.2." + (siteIndex + 1);
    }

    /**
     * The source address client site {@code siteIndex} binds in this
     * experiment: {@code geo.clientSiteBindHosts} when the config names them
     * (a distributed run, where the addresses must be routable from the server
     * machines), otherwise the single-host loopback convention above.
     */
    public String siteBindHost(int siteIndex) {
        if (geo.clientSiteBindHosts != null) {
            return geo.clientSiteBindHosts.get(siteIndex);
        }
        return clientSiteBindHost(siteIndex);
    }

    /**
     * Phases control offered load and which named mix the traffic is drawn
     * from: each injected request is drawn from the mix by weight, which
     * determines the application, whether it is a read or a write, and the
     * SLA it names. The consistency-level mix is no longer configured
     * anywhere: the decision policy chooses levels from the registered SLAs.
     */
    public static final class PhaseConfig {
        public String name;
        public Integer durationSeconds;
        public Integer totalTPS;
        public String mix;      // key into workload.mixes
    }

    /** One slice of a phase's traffic: a registered SLA and its relative weight. */
    public static final class SlaShare {
        public Integer applicationId;
        public String type;      // read | write
        public Integer slaId;
        public Double weight;    // relative to the other entries; > 0
    }

    /** True when the server-side scorer resolves the subSLA target (chameleon* modes). */
    public boolean chameleonDecision() {
        return mode.equals("chameleon") || mode.equals("chameleonPileus");
    }

    /** True when the client routes with the Pileus windows. */
    public boolean pileusRouting() {
        return mode.equals("chameleonPileus") || mode.equals("pileus");
    }

    // --- Loading ---

    /**
     * Shell entry point for run_all.sh: fully load and validate the config,
     * then print the facts the orchestrator needs as key=value lines. It
     * doubles as a preflight (a bad config fails here, before any process
     * starts) and uses the same strict parser the Java processes use, so the
     * two can never disagree. durationSeconds mirrors the driver's total
     * experiment duration: active phase time plus the drain buffer.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: ExperimentConfig <config.yaml|config.json>");
            System.exit(64);
        }
        ExperimentConfig config = load(Path.of(args[0]));
        long durationSeconds = config.workload.phases.stream().mapToLong(p -> p.durationSeconds).sum();
        System.out.println("numServers=" + config.cluster.numServers);
        System.out.println("durationSeconds=" + durationSeconds);
        System.out.println("serverHosts=" + String.join(",", config.cluster.serverHosts));
        System.out.println("geoEnabled=" + config.geo.enabled);
        System.out.println("transport=" + config.transport);
        if (config.mongodbTransport()) {
            System.out.println("mongodbPort=" + config.mongodb.port);
            System.out.println("mongodbReplicaSet=" + config.mongodb.replicaSet);
            System.out.println("mongodbMaxPoolSize=" + config.mongodb.maxPoolSize);
            System.out.println("mongodbPoolWaitTimeoutMs=" + config.mongodb.poolWaitTimeoutMs);
            System.out.println("mongodbServerHooks=" + config.mongodb.serverHooks);
            System.out.println("mongodbBinaryPath=" + (config.mongodb.binaryPath == null ? "" : config.mongodb.binaryPath));
            System.out.println("preferredLeaderId=" + config.cluster.preferredLeaderId);
        }
        if (config.geo.enabled) {
            // Matrix rows are ';'-separated, entries ','-separated, in ms.
            StringBuilder matrix = new StringBuilder();
            for (int i = 0; i < config.geo.interServerLatencyMs.length; i++) {
                if (i > 0) {
                    matrix.append(';');
                }
                Double[] row = config.geo.interServerLatencyMs[i];
                for (int j = 0; j < row.length; j++) {
                    matrix.append(j > 0 ? "," : "").append(row[j]);
                }
            }
            System.out.println("geoInterServerLatencyMs=" + matrix);
            // One row per client site (';'-separated), plus the loopback IPs
            // the sites bind - the runner's delay rules key on them.
            StringBuilder siteRows = new StringBuilder();
            StringBuilder siteHosts = new StringBuilder();
            StringBuilder siteNames = new StringBuilder();
            for (int s = 0; s < config.geo.clientSites.size(); s++) {
                ClientSite site = config.geo.clientSites.get(s);
                if (s > 0) {
                    siteRows.append(';');
                    siteHosts.append(',');
                    siteNames.append(',');
                }
                for (int j = 0; j < site.latencyMs.length; j++) {
                    siteRows.append(j > 0 ? "," : "").append(site.latencyMs[j]);
                }
                siteHosts.append(config.siteBindHost(s));
                siteNames.append(site.name);
            }
            System.out.println("geoClientSiteLatencyMs=" + siteRows);
            System.out.println("geoClientSiteHosts=" + siteHosts);
            System.out.println("geoClientSiteNames=" + siteNames);
        }
    }

    public static ExperimentConfig load(Path path) {
        String raw;
        try {
            raw = Files.readString(path);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read config file: " + path.toAbsolutePath(), e);
        }

        String fileName = path.getFileName().toString().toLowerCase();
        JsonElement root = (fileName.endsWith(".yaml") || fileName.endsWith(".yml"))
                ? parseYaml(raw, path)
                : parseJson(raw, path);
        if (!root.isJsonObject()) {
            throw new IllegalArgumentException("Config root must be a mapping/object: " + path.toAbsolutePath());
        }

        rejectUnknownKeys(root.getAsJsonObject(), ExperimentConfig.class, "");

        ExperimentConfig config = new Gson().fromJson(root, ExperimentConfig.class);
        config.validate();
        return config;
    }

    private static JsonElement parseJson(String raw, Path path) {
        try {
            return JsonParser.parseString(raw);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Config file is not valid JSON: " + path.toAbsolutePath() + " - " + e.getMessage(), e);
        }
    }

    private static JsonElement parseYaml(String raw, Path path) {
        Object tree;
        try {
            tree = new org.yaml.snakeyaml.Yaml(new org.yaml.snakeyaml.constructor.SafeConstructor(
                    new org.yaml.snakeyaml.LoaderOptions())).load(raw);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Config file is not valid YAML: " + path.toAbsolutePath() + " - " + e.getMessage(), e);
        }
        if (tree == null) {
            throw new IllegalArgumentException("Config file is empty: " + path.toAbsolutePath());
        }
        return yamlToJsonTree(tree);
    }

    /** Convert a SnakeYAML tree into a Gson tree so both formats share the strict loader. */
    private static JsonElement yamlToJsonTree(Object node) {
        if (node == null) {
            return com.google.gson.JsonNull.INSTANCE;
        }
        if (node instanceof Map<?, ?> map) {
            JsonObject obj = new JsonObject();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                obj.add(String.valueOf(e.getKey()), yamlToJsonTree(e.getValue()));
            }
            return obj;
        }
        if (node instanceof List<?> list) {
            com.google.gson.JsonArray array = new com.google.gson.JsonArray();
            for (Object item : list) {
                array.add(yamlToJsonTree(item));
            }
            return array;
        }
        if (node instanceof Boolean bool) {
            return new com.google.gson.JsonPrimitive(bool);
        }
        if (node instanceof Number number) {
            return new com.google.gson.JsonPrimitive(number);
        }
        return new com.google.gson.JsonPrimitive(node.toString());
    }

    /** Recursively reject keys that do not correspond to a schema field (typo protection). */
    private static void rejectUnknownKeys(JsonObject obj, Class<?> cls, String prefix) {
        Set<String> known = Arrays.stream(cls.getDeclaredFields()).map(Field::getName).collect(Collectors.toSet());
        for (String key : obj.keySet()) {
            if (!known.contains(key)) {
                throw new IllegalArgumentException("Unknown config key: '" + prefix + key + "'. Known keys at this level: " + known);
            }
            Field field;
            try {
                field = cls.getDeclaredField(key);
            } catch (NoSuchFieldException e) {
                throw new AssertionError(e);
            }
            JsonElement value = obj.get(key);
            if (value.isJsonObject() && Map.class.isAssignableFrom(field.getType())) {
                // Map keys are user-chosen names (e.g. mix names), so they are
                // not checked; each value is walked by the map's declared
                // value type so typos inside named entries are still rejected.
                java.lang.reflect.Type valueType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];
                for (Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
                    String entryPrefix = prefix + key + "." + entry.getKey();
                    if (valueType instanceof ParameterizedType listType
                            && List.class.isAssignableFrom((Class<?>) listType.getRawType())
                            && entry.getValue().isJsonArray()) {
                        Class<?> elementType = (Class<?>) listType.getActualTypeArguments()[0];
                        int i = 0;
                        for (JsonElement element : entry.getValue().getAsJsonArray()) {
                            if (element.isJsonObject()) {
                                rejectUnknownKeys(element.getAsJsonObject(), elementType, entryPrefix + "[" + i + "].");
                            }
                            i++;
                        }
                    } else if (valueType instanceof Class<?> valueClass && entry.getValue().isJsonObject()) {
                        rejectUnknownKeys(entry.getValue().getAsJsonObject(), valueClass, entryPrefix + ".");
                    }
                }
            } else if (value.isJsonObject()) {
                rejectUnknownKeys(value.getAsJsonObject(), field.getType(), prefix + key + ".");
            } else if (value.isJsonArray() && List.class.isAssignableFrom(field.getType())) {
                Class<?> elementType = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
                if (elementType != String.class) {
                    int i = 0;
                    for (JsonElement element : value.getAsJsonArray()) {
                        if (element.isJsonObject()) {
                            rejectUnknownKeys(element.getAsJsonObject(), elementType, prefix + key + "[" + i + "].");
                        }
                        i++;
                    }
                }
            }
        }
    }

    // --- Validation ---

    private static void require(Object value, String key) {
        if (value == null) {
            throw new IllegalArgumentException("Missing required config key: '" + key + "'");
        }
    }

    private static void requirePositive(Number value, String key) {
        require(value, key);
        if (value.doubleValue() <= 0) {
            throw new IllegalArgumentException("Config key '" + key + "' must be > 0, got " + value);
        }
    }

    private static final Set<String> READ_LEVELS = Set.of(
            "EVENTUAL_LOCAL", "EVENTUAL_MAJORITY", "CAUSAL_LOCAL", "CAUSAL_MAJORITY", "LINEARIZABLE");

    private static final Set<String> KEY_DISTRIBUTIONS = Set.of("uniform", "zipfian");

    /**
     * The experiment arms. Each value bundles who resolves the subSLA target
     * with how contact servers are chosen:
     * chameleon        - server scorer decides, lowest-RTT routing
     * chameleonPileus  - server scorer decides, Pileus routing
     * pileus           - client picks (server, rung) by expected profit
     * strongest        - client targets the strongest consistency rung, lowest-RTT routing
     * weakest          - client targets the weakest consistency rung, lowest-RTT routing
     */
    private static final Set<String> MODES = Set.of(
            "chameleon", "chameleonPileus", "pileus", "strongest", "weakest");

    /** Keep in sync with ClientMetricsTracker.MAX_RUNGS (fixed CSV columns). */
    private static final int MAX_RUNGS_PER_SLA = 4;

    private static void validateSla(Sla sla, String prefix, boolean isRead, int majority) {
        require(sla.slaId, prefix + "[].slaId");
        String slaPrefix = prefix + "[slaId=" + sla.slaId + "]";
        require(sla.rungs, slaPrefix + ".rungs");
        if (sla.rungs.isEmpty()) {
            throw new IllegalArgumentException(slaPrefix + ".rungs must not be empty");
        }
        if (sla.rungs.size() > MAX_RUNGS_PER_SLA) {
            throw new IllegalArgumentException(slaPrefix + ".rungs must have at most " + MAX_RUNGS_PER_SLA
                    + " rungs (the client ledger's satisfied-rung columns are fixed), got " + sla.rungs.size());
        }
        for (SlaRung rung : sla.rungs) {
            if (isRead) {
                require(rung.level, slaPrefix + ".rungs[].level");
                if (rung.concern != null) {
                    throw new IllegalArgumentException(slaPrefix + " is a read SLA; rungs must not set 'concern'");
                }
                if (!READ_LEVELS.contains(rung.level)) {
                    throw new IllegalArgumentException(slaPrefix + ".rungs[].level '" + rung.level
                            + "' is unknown. Valid: " + READ_LEVELS);
                }
            } else {
                require(rung.concern, slaPrefix + ".rungs[].concern");
                if (rung.level != null) {
                    throw new IllegalArgumentException(slaPrefix + " is a write SLA; rungs must not set 'level'");
                }
                if (rung.concern < 1 || rung.concern > majority) {
                    throw new IllegalArgumentException(slaPrefix + ".rungs[].concern " + rung.concern
                            + " is out of range [1, " + majority + "]");
                }
            }
            requirePositive(rung.latencyMs, slaPrefix + ".rungs[].latencyMs");
            requirePositive(rung.profit, slaPrefix + ".rungs[].profit");
        }
    }

    /** True when the workload runs against the patched MongoDB replica set instead of the Java store. */
    public boolean mongodbTransport() {
        return "mongodb".equals(transport);
    }

    /** True when the MongoDB transport relies on the server-side Chameleon module (parameters, reply envelope). */
    public boolean mongodbServerHooks() {
        return mongodbTransport() && !Boolean.FALSE.equals(mongodb.serverHooks);
    }

    private void validateTransport() {
        // Required, not defaulted. A config that had lost this line once ran a
        // whole mongod calibration ladder against the Java store: the default
        // took effect, every other key was valid, and the results were entirely
        // plausible. Which store a run drove is not something to infer.
        if (transport == null) {
            throw new IllegalArgumentException(
                    "transport is required and must be \"framed\" or \"mongodb\"");
        }
        if (!transport.equals("framed") && !transport.equals("mongodb")) {
            throw new IllegalArgumentException("transport must be \"framed\" or \"mongodb\", got '" + transport + "'");
        }
        if (!mongodbTransport()) {
            if (mongodb != null) {
                throw new IllegalArgumentException("mongodb block is only valid with transport: mongodb");
            }
            return;
        }
        require(mongodb, "mongodb");
        requirePositive(mongodb.port, "mongodb.port");
        require(mongodb.replicaSet, "mongodb.replicaSet");
        require(mongodb.database, "mongodb.database");
        require(mongodb.collection, "mongodb.collection");
        requirePositive(mongodb.maxPoolSize, "mongodb.maxPoolSize");
        requirePositive(mongodb.serverSelectionTimeoutMs, "mongodb.serverSelectionTimeoutMs");
        requirePositive(mongodb.poolWaitTimeoutMs, "mongodb.poolWaitTimeoutMs");
        requirePositive(mongodb.statusPollIntervalMs, "mongodb.statusPollIntervalMs");
        if (mongodb.port > 65_535) {
            throw new IllegalArgumentException("mongodb.port must be at or below 65535");
        }
        if (mongodb.serverHooks == null) {
            mongodb.serverHooks = true;
        }
        if (mongodb.binaryPath != null && mongodb.binaryPath.isBlank()) {
            throw new IllegalArgumentException("mongodb.binaryPath must be a path or omitted");
        }
        if (mongodb.binaryPath != null && mongodb.serverHooks) {
            throw new IllegalArgumentException("mongodb.binaryPath requires mongodb.serverHooks: false: a binary other"
                    + " than the patched experiment build has no Chameleon parameters to push");
        }
        if (!mongodb.serverHooks && !("weakest".equals(mode) || "strongest".equals(mode))) {
            throw new IllegalArgumentException("mongodb.serverHooks: false requires mode weakest or strongest: the"
                    + " chameleon modes need the server to decide, and pileus estimates staleness from the log and"
                    + " commit indices that only the $chameleon reply envelope carries (got mode '" + mode + "')");
        }
        if (server != null && Boolean.TRUE.equals(server.followerLinearizableReads)) {
            throw new IllegalArgumentException("server.followerLinearizableReads must be false with transport: mongodb:"
                    + " mongod serves linearizable reads on the primary only, so the client must route them there");
        }
        if (nodeFailure != null && Boolean.TRUE.equals(nodeFailure.enabled)) {
            throw new IllegalArgumentException(
                    "nodeFailure injection is not supported with transport: mongodb (no inter-server RPC drop hook)");
        }
        if (timedFailure != null && Boolean.TRUE.equals(timedFailure.enabled)) {
            throw new IllegalArgumentException(
                    "timedFailure injection is not supported with transport: mongodb (no inter-server RPC drop hook)");
        }
    }

    private void validate() {
        validateTransport();
        require(cluster, "cluster");
        requirePositive(cluster.numServers, "cluster.numServers");
        require(cluster.serverHosts, "cluster.serverHosts");
        if (cluster.serverHosts.size() != cluster.numServers) {
            throw new IllegalArgumentException("cluster.serverHosts must have exactly cluster.numServers ("
                    + cluster.numServers + ") entries, got " + cluster.serverHosts.size());
        }
        requirePositive(cluster.serverBasePort, "cluster.serverBasePort");
        requirePositive(cluster.clientBasePort, "cluster.clientBasePort");
        int raftLastPort = cluster.serverBasePort + cluster.numServers;
        int clientLastPort = cluster.clientBasePort + cluster.numServers;
        if (raftLastPort > 65_535 || clientLastPort > 65_535) {
            throw new IllegalArgumentException("cluster port ranges must end at or below 65535");
        }
        if (cluster.serverBasePort <= clientLastPort && cluster.clientBasePort <= raftLastPort) {
            throw new IllegalArgumentException("cluster.serverBasePort and cluster.clientBasePort ranges overlap");
        }
        require(cluster.preferredLeaderId, "cluster.preferredLeaderId");
        if (cluster.preferredLeaderId < -1 || cluster.preferredLeaderId >= cluster.numServers) {
            throw new IllegalArgumentException("cluster.preferredLeaderId must be -1 (no preference) or a server id"
                    + " in 0.." + (cluster.numServers - 1) + ", got " + cluster.preferredLeaderId);
        }

        require(mode, "mode");
        require(seed, "seed");
        if (!MODES.contains(mode)) {
            throw new IllegalArgumentException("mode must be one of " + MODES + ", got '" + mode + "'");
        }

        require(server, "server");
        requirePositive(server.maxWaitMs, "server.maxWaitMs");
        requirePositive(server.ingressHardCapInFlight, "server.ingressHardCapInFlight");
        requirePositive(server.sMax, "server.sMax");
        requirePositive(server.replicationBudgetPerSecond, "server.replicationBudgetPerSecond");
        requirePositive(server.maxEntriesPerReplicationBatch, "server.maxEntriesPerReplicationBatch");
        requirePositive(server.maxInflightReplicationBatchesPerFollower,
                "server.maxInflightReplicationBatchesPerFollower");
        requirePositive(server.replicationFlowControlWindowBytes,
                "server.replicationFlowControlWindowBytes");
        require(server.replicationLingerMs, "server.replicationLingerMs");
        if (!(server.replicationLingerMs >= 0) || !Double.isFinite(server.replicationLingerMs)) {
            throw new IllegalArgumentException("server.replicationLingerMs must be a non-negative finite"
                    + " number of milliseconds (0 disables the linger), got " + server.replicationLingerMs);
        }
        // Beyond the heartbeat there is no batching left to win and every
        // majority write pays the wait; the tightest write rung in the repo
        // configs allows 120 ms end to end.
        if (server.replicationLingerMs > 20) {
            throw new IllegalArgumentException("server.replicationLingerMs must not exceed 20 ms (the"
                    + " heartbeat interval, which already bounds how long a batch can wait), got "
                    + server.replicationLingerMs);
        }
        if (server.replicationFlowControlWindowBytes < 65536) {
            throw new IllegalArgumentException(
                    "server.replicationFlowControlWindowBytes must be at least 65536 (the HTTP/2 default); got "
                            + server.replicationFlowControlWindowBytes);
        }
        require(server.followerLinearizableReads, "server.followerLinearizableReads");
        require(server.histogramDecay, "server.histogramDecay");
        if (!(server.histogramDecay > 0) || !(server.histogramDecay < 1)) {
            throw new IllegalArgumentException("server.histogramDecay must be in (0, 1), got "
                    + server.histogramDecay);
        }

        require(chameleon, "chameleon");
        requirePositive(chameleon.controlIntervalMs, "chameleon.controlIntervalMs");
        requirePositive(chameleon.uTarget, "chameleon.uTarget");
        requirePositive(chameleon.eta, "chameleon.eta");
        // lambdaMin = 0 is a deliberate ablation, not a degenerate config:
        // zero is absorbing under the multiplicative price update, so the
        // shadow price stays exactly 0 for the whole run - a "priceless"
        // chameleon where the scorer still decides levels but admission is
        // only the hard occupancy cap and every upgrade is free.
        require(chameleon.lambdaMin, "chameleon.lambdaMin");
        if (!(chameleon.lambdaMin >= 0) || !Double.isFinite(chameleon.lambdaMin)) {
            throw new IllegalArgumentException("chameleon.lambdaMin must be a non-negative finite number"
                    + " (0 pins the price at zero for the whole run), got " + chameleon.lambdaMin);
        }
        // Anti-windup. Admission keeps a level only while its expected profit
        // exceeds lambda * omega, so beyond max_rung_profit / omega the price
        // already refuses everything and more of it changes no decision - but
        // the controller does not know that and keeps multiplying, because
        // in-flight is bounded by the ingress cap rather than by the price, so
        // u stays above target however high lambda goes. Unbounded, that
        // reached Double.POSITIVE_INFINITY on the local topology 45 s into a
        // 2.5C phase, and infinity is absorbing exactly as zero is: that node
        // refused every request for the remaining 41% of the run.
        require(chameleon.lambdaMax, "chameleon.lambdaMax");
        if (!Double.isFinite(chameleon.lambdaMax) || !(chameleon.lambdaMax > chameleon.lambdaMin)) {
            throw new IllegalArgumentException("chameleon.lambdaMax must be a finite number greater than"
                    + " chameleon.lambdaMin (" + chameleon.lambdaMin + "), got " + chameleon.lambdaMax);
        }

        require(client, "client");
        requirePositive(client.rttWindowSize, "client.rttWindowSize");
        if (client.rttWindowSize < 8) {
            throw new IllegalArgumentException("client.rttWindowSize must be >= 8, got " + client.rttWindowSize);
        }
        requirePositive(client.retryLimit, "client.retryLimit");
        requirePositive(client.lostTimeoutMs, "client.lostTimeoutMs");
        requirePositive(client.ingressConnectionsPerServer, "client.ingressConnectionsPerServer");
        require(client.explorationFraction, "client.explorationFraction");
        if (client.explorationFraction < 0 || client.explorationFraction >= 1) {
            throw new IllegalArgumentException("client.explorationFraction must be in [0, 1), got "
                    + client.explorationFraction);
        }
        require(client.admissionAwareRouting, "client.admissionAwareRouting");
        require(client.admitRateGamma, "client.admitRateGamma");
        if (!(client.admitRateGamma > 0) || !(client.admitRateGamma < 1)) {
            throw new IllegalArgumentException("client.admitRateGamma must be in (0, 1), got "
                    + client.admitRateGamma);
        }

        require(workload, "workload");
        requirePositive(workload.keySpace, "workload.keySpace");
        require(workload.keyDistribution, "workload.keyDistribution");
        if (!KEY_DISTRIBUTIONS.contains(workload.keyDistribution)) {
            throw new IllegalArgumentException("workload.keyDistribution must be one of " + KEY_DISTRIBUTIONS
                    + ", got '" + workload.keyDistribution + "'");
        }
        if (workload.keyDistribution.equals("zipfian")) {
            requirePositive(workload.zipfianExponent, "workload.zipfianExponent");
        }
        requirePositive(workload.sessionsPerApplication, "workload.sessionsPerApplication");
        requirePositive(workload.injectorThreads, "workload.injectorThreads");
        // Sessions are sharded across injector threads; every thread must own
        // at least one session of every application or it could inject nothing.
        if (workload.injectorThreads > workload.sessionsPerApplication) {
            throw new IllegalArgumentException("workload.injectorThreads must be <= workload.sessionsPerApplication ("
                    + workload.sessionsPerApplication + "), got " + workload.injectorThreads);
        }

        require(slas, "slas");
        if (slas.isEmpty()) {
            throw new IllegalArgumentException("slas must register at least one application");
        }
        // The workload driver indexes session pools by applicationId - 1, so
        // ids must be exactly 1..N.
        java.util.Set<Integer> appIds = new java.util.HashSet<>();
        for (AppSlas app : slas) {
            require(app.applicationId, "slas[].applicationId");
            appIds.add(app.applicationId);
        }
        for (int expected = 1; expected <= slas.size(); expected++) {
            if (!appIds.contains(expected)) {
                throw new IllegalArgumentException("slas applicationIds must be exactly 1.." + slas.size()
                        + " (distinct, contiguous), got " + new java.util.TreeSet<>(appIds));
            }
        }
        int majority = (cluster.numServers / 2) + 1;
        for (AppSlas app : slas) {
            require(app.applicationId, "slas[].applicationId");
            String appPrefix = "slas[applicationId=" + app.applicationId + "]";
            require(app.read, appPrefix + ".read");
            require(app.write, appPrefix + ".write");
            if (app.read.isEmpty() || app.write.isEmpty()) {
                throw new IllegalArgumentException(appPrefix + " must register at least one read and one write SLA");
            }
            for (Sla sla : app.read) {
                validateSla(sla, appPrefix + ".read", true, majority);
            }
            for (Sla sla : app.write) {
                validateSla(sla, appPrefix + ".write", false, majority);
            }
        }

        require(nodeFailure, "nodeFailure");
        require(nodeFailure.enabled, "nodeFailure.enabled");
        require(nodeFailure.failedNodeId, "nodeFailure.failedNodeId");
        if (nodeFailure.failedNodeId < 0 || nodeFailure.failedNodeId >= cluster.numServers) {
            throw new IllegalArgumentException("nodeFailure.failedNodeId must be in [0, " + (cluster.numServers - 1) + "]");
        }

        require(timedFailure, "timedFailure");
        require(timedFailure.enabled, "timedFailure.enabled");
        require(timedFailure.targetRole, "timedFailure.targetRole");
        if (!timedFailure.targetRole.equals("LEADER") && !timedFailure.targetRole.equals("FOLLOWER")) {
            throw new IllegalArgumentException("timedFailure.targetRole must be LEADER or FOLLOWER, got '" + timedFailure.targetRole + "'");
        }
        requirePositive(timedFailure.afterSeconds, "timedFailure.afterSeconds");

        require(geo, "geo");
        require(geo.enabled, "geo.enabled");
        require(geo.interServerLatencyMs, "geo.interServerLatencyMs");
        if (geo.interServerLatencyMs.length != cluster.numServers) {
            throw new IllegalArgumentException("geo.interServerLatencyMs must have " + cluster.numServers
                    + " rows (one per server), got " + geo.interServerLatencyMs.length);
        }
        for (int i = 0; i < geo.interServerLatencyMs.length; i++) {
            Double[] row = geo.interServerLatencyMs[i];
            require(row, "geo.interServerLatencyMs[" + i + "]");
            if (row.length != cluster.numServers) {
                throw new IllegalArgumentException("geo.interServerLatencyMs[" + i + "] must have "
                        + cluster.numServers + " entries (one per server), got " + row.length);
            }
            for (int j = 0; j < row.length; j++) {
                require(row[j], "geo.interServerLatencyMs[" + i + "][" + j + "]");
                if (!(row[j] >= 0) || !Double.isFinite(row[j])) {
                    throw new IllegalArgumentException("geo.interServerLatencyMs[" + i + "][" + j
                            + "] must be a non-negative finite number, got " + row[j]);
                }
                if (i == j && row[j] != 0) {
                    throw new IllegalArgumentException("geo.interServerLatencyMs[" + i + "][" + j
                            + "] is a server's latency to itself and must be 0, got " + row[j]);
                }
            }
        }
        require(geo.clientSites, "geo.clientSites");
        if (geo.clientSites.isEmpty()) {
            throw new IllegalArgumentException("geo.clientSites must define at least one client site");
        }
        if (geo.clientSites.size() > 254) {
            throw new IllegalArgumentException("geo.clientSites supports at most 254 sites"
                    + " (each binds one 127.0.2.x source IP), got " + geo.clientSites.size());
        }
        java.util.Set<String> siteNames = new java.util.HashSet<>();
        for (int s = 0; s < geo.clientSites.size(); s++) {
            ClientSite site = geo.clientSites.get(s);
            String sitePrefix = "geo.clientSites[" + s + "]";
            require(site, sitePrefix);
            require(site.name, sitePrefix + ".name");
            // Names end up in CSV cells and shell-parsed key=value lines.
            if (!site.name.matches("[A-Za-z0-9._-]+")) {
                throw new IllegalArgumentException(sitePrefix + ".name must match [A-Za-z0-9._-]+, got '"
                        + site.name + "'");
            }
            if (!siteNames.add(site.name)) {
                throw new IllegalArgumentException(sitePrefix + ".name '" + site.name + "' is duplicated");
            }
            require(site.latencyMs, sitePrefix + ".latencyMs");
            if (site.latencyMs.length != cluster.numServers) {
                throw new IllegalArgumentException(sitePrefix + ".latencyMs must have " + cluster.numServers
                        + " entries (one per server), got " + site.latencyMs.length);
            }
            for (int j = 0; j < site.latencyMs.length; j++) {
                Double v = site.latencyMs[j];
                require(v, sitePrefix + ".latencyMs[" + j + "]");
                if (!(v >= 0) || !Double.isFinite(v)) {
                    throw new IllegalArgumentException(sitePrefix + ".latencyMs[" + j
                            + "] must be a non-negative finite number, got " + v);
                }
            }
        }
        // Optional bind-host override: one routable source address per site,
        // written by run_all_cloudlab.sh for a distributed run. Absent means
        // the single-host loopback convention.
        if (geo.clientSiteBindHosts != null) {
            if (geo.clientSiteBindHosts.size() != geo.clientSites.size()) {
                throw new IllegalArgumentException("geo.clientSiteBindHosts must have one entry per client site ("
                        + geo.clientSites.size() + "), got " + geo.clientSiteBindHosts.size());
            }
            java.util.Set<String> bindHosts = new java.util.HashSet<>();
            for (int s2 = 0; s2 < geo.clientSiteBindHosts.size(); s2++) {
                String host = geo.clientSiteBindHosts.get(s2);
                require(host, "geo.clientSiteBindHosts[" + s2 + "]");
                // Dotted-quad only: the delay rules match on literal addresses,
                // and a hostname here would resolve differently per machine.
                if (!host.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
                    throw new IllegalArgumentException("geo.clientSiteBindHosts[" + s2
                            + "] must be a literal IPv4 address, got '" + host + "'");
                }
                if (!bindHosts.add(host)) {
                    throw new IllegalArgumentException("geo.clientSiteBindHosts[" + s2 + "] '" + host
                            + "' is duplicated; each site needs its own source address so the delay rules"
                            + " can tell sites apart");
                }
            }
        }
        // Sessions are spread evenly across sites (EXPERIMENT.md); an uneven
        // split would silently skew per-site load, so it is rejected instead.
        if (workload.sessionsPerApplication % geo.clientSites.size() != 0) {
            throw new IllegalArgumentException("workload.sessionsPerApplication ("
                    + workload.sessionsPerApplication + ") must be divisible by the number of client sites ("
                    + geo.clientSites.size() + ") so sessions spread evenly across sites");
        }
        // No host-shape requirements for geo here: run_all_cloudlab.sh
        // rewrites cluster.serverHosts to distinct literal loopback IPs
        // (127.0.1.1..N) before the config reaches any process, which is what
        // the delay rules and the servers' source binding key on. A local
        // run_all.sh run refuses geo-enabled configs outright.

        // Mixes and phases live under workload but are validated here, after
        // slas, because every mix entry must name a registered SLA.
        java.util.Set<String> registeredSlas = new java.util.HashSet<>();
        for (AppSlas app : slas) {
            for (Sla sla : app.read) {
                registeredSlas.add(app.applicationId + "/read/" + sla.slaId);
            }
            for (Sla sla : app.write) {
                registeredSlas.add(app.applicationId + "/write/" + sla.slaId);
            }
        }
        require(workload.mixes, "workload.mixes");
        if (workload.mixes.isEmpty()) {
            throw new IllegalArgumentException("workload.mixes must define at least one mix");
        }
        for (Map.Entry<String, List<SlaShare>> mix : workload.mixes.entrySet()) {
            String mixPrefix = "workload.mixes." + mix.getKey();
            require(mix.getValue(), mixPrefix);
            if (mix.getValue().isEmpty()) {
                throw new IllegalArgumentException(mixPrefix + " must contain at least one entry");
            }
            for (int j = 0; j < mix.getValue().size(); j++) {
                SlaShare share = mix.getValue().get(j);
                String sharePrefix = mixPrefix + "[" + j + "]";
                require(share.applicationId, sharePrefix + ".applicationId");
                require(share.type, sharePrefix + ".type");
                require(share.slaId, sharePrefix + ".slaId");
                require(share.weight, sharePrefix + ".weight");
                if (!share.type.equals("read") && !share.type.equals("write")) {
                    throw new IllegalArgumentException(sharePrefix + ".type must be read or write, got '"
                            + share.type + "'");
                }
                if (!(share.weight > 0) || !Double.isFinite(share.weight)) {
                    throw new IllegalArgumentException(sharePrefix + ".weight must be a positive finite number, got "
                            + share.weight);
                }
                String key = share.applicationId + "/" + share.type + "/" + share.slaId;
                if (!registeredSlas.contains(key)) {
                    throw new IllegalArgumentException(sharePrefix + " references unregistered SLA " + key
                            + " (registered: " + new java.util.TreeSet<>(registeredSlas) + ")");
                }
            }
        }
        List<PhaseConfig> phases = workload.phases;
        require(phases, "workload.phases");
        if (phases.isEmpty()) {
            throw new IllegalArgumentException("workload.phases must contain at least one phase");
        }
        for (int i = 0; i < phases.size(); i++) {
            PhaseConfig p = phases.get(i);
            String prefix = "workload.phases[" + i + "]";
            require(p.name, prefix + ".name");
            requirePositive(p.durationSeconds, prefix + ".durationSeconds");
            requirePositive(p.totalTPS, prefix + ".totalTPS");
            require(p.mix, prefix + ".mix");
            if (!workload.mixes.containsKey(p.mix)) {
                throw new IllegalArgumentException(prefix + ".mix '" + p.mix + "' is not defined in workload.mixes "
                        + new java.util.TreeSet<>(workload.mixes.keySet()));
            }
        }
    }
}
