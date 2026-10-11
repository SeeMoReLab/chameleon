package org.example.Client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.example.Utility.ExperimentConfig;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.Client.KvTransport.Cause;
import org.example.raft.ReadLevel;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.TransportSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;

import io.grpc.Status;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * KvTransport over the MongoDB wire protocol, against the patched mongod
 * (MONGODB_PORT.md, Milestone 3). CLIENTS_PER_SITE MongoClients per client
 * site, all sharing one Netty event loop; the site's source address is bound
 * inside the Netty socket channel so the netem delay rules can match per
 * (site, server) pair without a process or namespace per site.
 *
 * Every request is a raw runCommand so the reply comes back whole, including
 * the {@code $chameleon} envelope the patched server appends; the fluent
 * driver API would discard it. The node is addressed with a tag set
 * ({node: "k"}) that the replica set configuration assigns to member k, so
 * the driver's own server selection collapses to the node the harness chose.
 * Writes sent to a secondary fail with NotWritablePrimary, which is the
 * redirect; the leader hint is the primary the driver's topology monitor
 * currently knows.
 *
 * Request envelope, carried in the standard {@code comment} argument:
 * {app, sla, rtt, read, frontier?, target?, wantLin, wc, waitBound}; rtt is the
 * client's network estimate plus the checkout wait of the pool the request
 * is about to use (see execute).
 * Reply envelope {@code $chameleon}: {level, wc, lastApplied, commit, readTs,
 * entryTs, serviceMs, predicted, waited, fellBack, rejected, epochMs}.
 * Timestamps travel as BSON timestamps and become the int64 (secs << 32 | inc)
 * value on the KvResponse, which is the session frontier's unit.
 */
public final class MongoTransport implements KvTransport {

    /** Error code the patched server uses when admission sheds a request (base/error_codes.yml). */
    public static final String REJECTED_CODE_NAME = "ChameleonRejected";
    private static final int NOT_WRITABLE_PRIMARY = 10107;
    private static final int NOT_PRIMARY_NO_SECONDARY_OK = 13435;
    private static final int NOT_PRIMARY_OR_SECONDARY = 13436;
    private static final int MAX_TIME_MS_EXPIRED = 50;
    private static final String ENVELOPE_FIELD = "$chameleon";

    public record Settings(
            List<String> hosts,
            int port,
            String replicaSet,
            String database,
            String collection,
            int maxPoolSize,
            int serverSelectionTimeoutMs,
            /**
             * How long a checkout waits for a pooled connection. Pools grow on demand and an
             * open costs two round trips, so this must cover the slowest node's open (up to
             * 640 ms on the global topology) or every checkout that lands on a growing pool fails.
             */
            int poolWaitTimeoutMs,
            /** One entry per site; null = unbound source address (geo disabled). */
            List<String> siteBindHosts,
            /** One entry per site, for labelling driver events. */
            List<String> siteNames,
            /**
             * Whether replies carry the patched server's $chameleon envelope. False grades
             * from the driver's own reply metadata (mongodb.serverHooks: false).
             */
            boolean serverHooks) {

        /**
         * Acknowledgments a majority write needs: a majority of the voting members, of which
         * MongoDB allows at most seven (ExperimentConfig.mongodbWriteMajority).
         */
        int majority() {
            return ExperimentConfig.mongodbWriteMajority(hosts.size());
        }

        /** Each client's share of maxPoolSize, which bounds a (site, server) pair across the site's clients. */
        int poolSizePerClient() {
            return (maxPoolSize + CLIENTS_PER_SITE - 1) / CLIENTS_PER_SITE;
        }
    }

    /**
     * Driver clients per client site. The driver serializes every connection checkout of a
     * pool through that pool's single AsyncGetter thread, and a client keeps one pool per
     * server, so with one client per site a (site, server) pair could check out only as fast
     * as one thread: about 40-50k/s on a c6525-25g client. Nearest-node routing sends a
     * site's whole read stream through one pair, and the Flash phase exceeds that. The
     * getter's queue then grows, its live tasks push young-GC pauses from ~3 ms to ~200 ms,
     * the whole client stalls, and checkouts time out while the servers sit idle (MongoDB
     * scale 3 Flash: 36% of wall time in GC pauses, paths that served nothing for 50 s).
     * The site's sessions are spread evenly over this many clients, each holding
     * poolSizePerClient() connections, so a pair's connection budget is unchanged and its
     * checkout ceiling is this many times higher.
     *
     * Each session stays on one client (forSession), never round-robin per request. A
     * client gossips its own cluster clock, and a secondary refuses an afterClusterTime
     * ahead of the clock a read arrives with (InvalidOptions); a session's frontier comes
     * from its own replies, so its client's clock always covers it, as it did with one
     * client per site. Round-robin broke that: 1,707 refused causal reads in one short run.
     */
    static final int CLIENTS_PER_SITE = 4;

    private final Settings settings;
    private final EventLoopGroup eventLoopGroup;
    /** [site][client] */
    private final MongoClient[][] siteClients;
    private final MongoDriverEvents[][] siteEvents;
    private final MongoDatabase[][] siteDatabases;
    private final ReadPreference[] nodePreferences;
    /** Per node, the address string the driver's pool events are keyed by. */
    private final String[] nodeAddresses;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MongoTransport(Settings settings) {
        this.settings = Objects.requireNonNull(settings);
        if (settings.siteBindHosts().size() > SiteBoundChannel.MAX_SITES) {
            throw new IllegalArgumentException("MongoTransport supports at most " + SiteBoundChannel.MAX_SITES
                    + " client sites (one bound Netty channel class each), got " + settings.siteBindHosts().size());
        }
        SiteBoundChannel.install(settings.siteBindHosts());
        this.eventLoopGroup = new NioEventLoopGroup();
        List<ServerAddress> seeds = new ArrayList<>();
        for (String host : settings.hosts()) {
            seeds.add(new ServerAddress(host, settings.port()));
        }
        int numSites = settings.siteBindHosts().size();
        this.siteClients = new MongoClient[numSites][CLIENTS_PER_SITE];
        this.siteDatabases = new MongoDatabase[numSites][CLIENTS_PER_SITE];
        this.siteEvents = new MongoDriverEvents[numSites][CLIENTS_PER_SITE];
        for (int site = 0; site < numSites; site++) {
            for (int c = 0; c < CLIENTS_PER_SITE; c++) {
                siteEvents[site][c] = new MongoDriverEvents(settings.siteNames().get(site) + "/" + c);
                MongoClientSettings clientSettings = clientSettings(settings, seeds, TransportSettings.nettyBuilder()
                        .eventLoopGroup(eventLoopGroup)
                        .socketChannelClass(SiteBoundChannel.classForSite(site))
                        .build(), siteEvents[site][c]);
                siteClients[site][c] = MongoClients.create(clientSettings);
                siteDatabases[site][c] = siteClients[site][c].getDatabase(settings.database());
            }
        }
        this.nodePreferences = new ReadPreference[settings.hosts().size()];
        this.nodeAddresses = new String[settings.hosts().size()];
        for (int node = 0; node < nodePreferences.length; node++) {
            nodePreferences[node] = ReadPreference.nearest(new TagSet(new Tag("node", Integer.toString(node))));
            nodeAddresses[node] = new ServerAddress(settings.hosts().get(node), settings.port()).toString();
        }
    }

    /**
     * Whether a request with {@code remainingMs} left should be refused rather than
     * queued for a connection to {@code address}. All three conditions are needed.
     * The pool must already hold its full complement, since a pool that can still grow
     * makes a waiting request productive. There must be a queue on it right now, or the
     * smoothed wait latches: a pool whose requests are all refused performs no
     * checkouts, so nothing fresh can contradict a stale high mean, and it locks out
     * permanently. And the queue must have been taking longer than the deadline, since
     * waiting out a queue longer than that misses it and holds a slot meanwhile.
     */
    static boolean refuseForQueue(MongoDriverEvents events, String address, int maxPoolSize, long remainingMs) {
        return events.poolSaturated(address, maxPoolSize)
                && events.queueDepth(address) > 0
                && events.checkoutWaitMs(address) > remainingMs;
    }

    /** A view pinned to one of each site's clients; every session sends through one (CLIENTS_PER_SITE). */
    @Override
    public KvTransport forSession(int sessionKey) {
        return new SessionView(Math.floorMod(sessionKey, CLIENTS_PER_SITE));
    }

    /** Refused: requests must go through forSession, which picks the client and so the cluster clock. */
    @Override
    public double queueWaitMs(int siteId, int nodeId) {
        throw new UnsupportedOperationException("MongoTransport is used through forSession(sessionKey)");
    }

    /** Refused: requests must go through forSession, which picks the client and so the cluster clock. */
    @Override
    public RequestHandle execute(int siteId, int nodeId, KvRequest request, long remainingDeadlineNanos,
            Consumer<KvResponse> onResponse, Consumer<RpcFailure> onError) {
        throw new UnsupportedOperationException("MongoTransport is used through forSession(sessionKey)");
    }

    /** One session's path: the shared transport with the client fixed. Closing it is the transport owner's job. */
    private final class SessionView implements KvTransport {
        private final int client;

        SessionView(int client) {
            this.client = client;
        }

        @Override
        public int writeMajority() {
            return MongoTransport.this.writeMajority();
        }

        @Override
        public int numServers() {
            return MongoTransport.this.numServers();
        }

        @Override
        public int numSites() {
            return MongoTransport.this.numSites();
        }

        @Override
        public double queueWaitMs(int siteId, int nodeId) {
            return siteEvents[siteId][client].checkoutWaitMs(nodeAddresses[nodeId]);
        }

        @Override
        public RequestHandle execute(int siteId, int nodeId, KvRequest request, long remainingDeadlineNanos,
                Consumer<KvResponse> onResponse, Consumer<RpcFailure> onError) {
            return MongoTransport.this.execute(siteId, client, nodeId, request, remainingDeadlineNanos,
                    onResponse, onError);
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("a session view does not own the MongoTransport");
        }
    }

    /** Driver settings for one of a site's clients; the transport is the site's bound Netty channel. */
    static MongoClientSettings clientSettings(Settings settings, List<ServerAddress> seeds, TransportSettings transport,
            MongoDriverEvents events) {
        return MongoClientSettings.builder()
                .applyToClusterSettings(b -> b.hosts(seeds)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredReplicaSetName(settings.replicaSet())
                        .serverSelectionTimeout(settings.serverSelectionTimeoutMs(), TimeUnit.MILLISECONDS)
                        .addClusterListener(events))
                .applyToServerSettings(b -> b.addServerListener(events).addServerMonitorListener(events))
                // maxConnecting defaults to 2 concurrent opens per pool, which under geo latency
                // lets a far pool grow by only a few connections per second while checkouts queue
                // behind the opens and time out; a pool must reach its demand within one open time.
                .applyToConnectionPoolSettings(b -> b.maxSize(settings.poolSizePerClient())
                        .maxConnecting(settings.poolSizePerClient())
                        .maxWaitTime(settings.poolWaitTimeoutMs(), TimeUnit.MILLISECONDS)
                        .addConnectionPoolListener(events))
                .transportSettings(transport)
                // The harness owns retries: a driver retry would hide redirects and double count.
                .retryReads(false)
                .retryWrites(false)
                .build();
    }

    @Override
    public int writeMajority() {
        return settings.majority();
    }

    @Override
    public int numServers() {
        return settings.hosts().size();
    }

    @Override
    public int numSites() {
        return siteClients.length;
    }

    /** KvTransport.execute on the site's {@code client}; reached through a SessionView. */
    private RequestHandle execute(int siteId, int client, int nodeId, KvRequest request, long remainingDeadlineNanos,
            Consumer<KvResponse> onResponse, Consumer<RpcFailure> onError) {
        if (closed.get()) {
            onError.accept(new RpcFailure(Status.UNAVAILABLE.withDescription("mongo transport is closed"), 0));
            return () -> { };
        }
        if (siteId < 0 || siteId >= siteClients.length) {
            throw new IllegalArgumentException("invalid siteId " + siteId);
        }
        if (nodeId < 0 || nodeId >= nodePreferences.length) {
            throw new IllegalArgumentException("invalid nodeId " + nodeId);
        }
        if (remainingDeadlineNanos <= 0) {
            onError.accept(new RpcFailure(
                    Status.DEADLINE_EXCEEDED.withDescription("SLA deadline expired before dispatch"), 0));
            return () -> { };
        }
        long maxTimeMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingDeadlineNanos));
        // Do not enter a queue longer than the deadline. The driver's pool wait is one
        // setting for every request, so it has to cover a cold pool's first connection
        // (two round trips to a far node) and is therefore far longer than most SLA
        // deadlines; a request that waits it out has missed its deadline before it
        // reaches mongod, and while it waits it holds a queue slot that a request with
        // a longer deadline could have used. Only a pool that cannot grow is refused:
        // while it is still opening connections the wait is productive.
        MongoDriverEvents events = siteEvents[siteId][client];
        String address = nodeAddresses[nodeId];
        if (refuseForQueue(events, address, settings.poolSizePerClient(), maxTimeMs)) {
            onError.accept(new RpcFailure(Status.DEADLINE_EXCEEDED.withDescription(
                    "connection pool at " + address + " is full and its checkout queue is "
                            + Math.round(events.checkoutWaitMs(address)) + " ms against " + maxTimeMs
                            + " ms of deadline left"), 0, Cause.TRANSPORT_QUEUE));
            return () -> { };
        }
        // The scorer subtracts the envelope's rtt from every rung's threshold as the
        // budget already spent outside mongod. The client's own estimate is network
        // only (KvSessionClient.observeRtt takes this pool's wait back out so that
        // routing is not poisoned by the client's queues), but the request about to
        // be queued on this pool will spend that wait before mongod starts its clock,
        // and the client grades it from dispatch. Charging the wait here, per pool,
        // lets the server see the request from the moment the client started
        // counting; a saturated primary pool then raises rtt only for requests bound
        // for the primary, and admission-aware routing moves the rest away.
        double queueWaitMs = events.checkoutWaitMs(address);
        Document command = request.getIsRead()
                ? findCommand(request, maxTimeMs, queueWaitMs)
                : updateCommand(request, maxTimeMs, queueWaitMs);
        Publisher<Document> publisher = siteDatabases[siteId][client].runCommand(command, nodePreferences[nodeId]);

        CommandSubscriber subscriber = new CommandSubscriber(siteId, client, request, onResponse, onError);
        publisher.subscribe(subscriber);
        return subscriber::cancel;
    }

    /** What the driver did to its cluster view during the run, per site client; empty when nothing notable. */
    public String driverEventSummary() {
        StringBuilder sb = new StringBuilder();
        for (MongoDriverEvents[] site : siteEvents) {
            for (MongoDriverEvents events : site) {
                sb.append(events.summary());
            }
        }
        return sb.toString();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (MongoClient[] site : siteClients) {
            for (MongoClient client : site) {
                client.close();
            }
        }
        eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }

    // ===== Request translation =====

    /** Frontier as a BSON timestamp; null when the session has observed nothing (-1). */
    static BsonTimestamp frontierOf(KvRequest request) {
        long frontier = request.getSessionIndex();
        return frontier < 0 ? null : new BsonTimestamp(frontier);
    }

    /**
     * The request envelope. {@code queueWaitMs} is the checkout wait of the pool this
     * request is about to use; it is added to the client's network RTT estimate so
     * the server's rho covers everything the request spends outside mongod.
     */
    static Document envelope(KvRequest request, double queueWaitMs) {
        if (!(queueWaitMs >= 0) || Double.isNaN(queueWaitMs)) {
            throw new IllegalArgumentException("pool checkout wait must be a non-negative number of ms, got " + queueWaitMs);
        }
        Document env = new Document("app", request.getApplicationId())
                .append("sla", request.getSlaId())
                .append("rtt", request.getRttEstimateMs() + queueWaitMs)
                .append("read", request.getIsRead());
        BsonTimestamp frontier = frontierOf(request);
        if (frontier != null) {
            env.append("frontier", frontier);
        }
        if (request.hasTargetReadLevel()) {
            env.append("target", request.getTargetReadLevel().getNumber());
        }
        env.append("wantLin", request.getWantLinearizable())
                .append("wc", request.getRequestedWriteConcern())
                .append("waitBound", request.getWaitBoundMs());
        return env;
    }

    /**
     * Read concern for a client-decided target level. Absent (null) means the
     * server decides (chameleon modes). An untargeted client-decided read maps
     * to majority: the KV store's client-decided path serves the committed
     * view, and majority is its MongoDB equivalent.
     */
    static Document readConcernFor(KvRequest request) {
        if (!request.hasTargetReadLevel()) {
            return request.getWantLinearizable() ? new Document("level", "linearizable") : null;
        }
        BsonTimestamp frontier = frontierOf(request);
        return switch (request.getTargetReadLevel()) {
            case EVENTUAL_LOCAL -> new Document("level", "local");
            case EVENTUAL_MAJORITY -> new Document("level", "majority");
            case CAUSAL_LOCAL -> withFrontier(new Document("level", "local"), frontier);
            case CAUSAL_MAJORITY -> withFrontier(new Document("level", "majority"), frontier);
            case LINEARIZABLE -> new Document("level", "linearizable");
            case UNRECOGNIZED -> throw new IllegalArgumentException(
                    "unrecognized target read level " + request.getTargetReadLevelValue());
        };
    }

    private static Document withFrontier(Document readConcern, BsonTimestamp frontier) {
        if (frontier != null) {
            readConcern.append("afterClusterTime", frontier);
        }
        return readConcern;
    }

    Document findCommand(KvRequest request, long maxTimeMs, double queueWaitMs) {
        Document command = new Document("find", settings.collection())
                .append("filter", new Document("_id", request.getKey()))
                .append("limit", 1)
                .append("singleBatch", true)
                .append("maxTimeMS", maxTimeMs)
                .append("comment", envelope(request, queueWaitMs));
        Document readConcern = readConcernFor(request);
        if (readConcern != null) {
            command.append("readConcern", readConcern);
        }
        return command;
    }

    Document updateCommand(KvRequest request, long maxTimeMs, double queueWaitMs) {
        return updateCommand(request, maxTimeMs, queueWaitMs, settings.collection(), settings.serverHooks(),
                settings.majority());
    }

    /**
     * A write never carries maxTimeMS. A writeConcernError whose code is in the
     * execution-timeout category (maxTimeMS expiring during the replication wait,
     * code 50) makes the Java driver mark the server UNKNOWN, and every request bound
     * for the primary from that site then fails server selection until the next
     * heartbeat; under overload the patched server's clamp of the wait inside the
     * deadline was not enough to prevent it (MONGODB_PORT.md, Milestone 3 item 8). So
     * the acknowledgment wait is bounded on the server alone: with the hooks on by the
     * envelope's waitBound and the server's maxWaitMs, without them by wtimeout set to
     * the remaining deadline (an ordinary code 64). A write that is late is graded late
     * by the client, which is also how the Java store treats its writes.
     */
    static Document updateCommand(KvRequest request, long maxTimeMs, double queueWaitMs, String collection,
            boolean serverHooks, int majority) {
        Document update = new Document("q", new Document("_id", request.getKey()))
                .append("u", new Document("$set", new Document("v", request.getValue())))
                .append("upsert", true)
                .append("multi", false);
        Document command = new Document("update", collection)
                .append("updates", List.of(update))
                .append("ordered", true);
        command.append("comment", envelope(request, queueWaitMs));
        int wc = request.getRequestedWriteConcern();
        if (wc > 0) {
            // Client-decided concern; with the hooks on the server clamps it to the
            // majority and bounds the wait by the envelope's waitBound. A concern at the
            // majority goes as w:"majority", not as a count: a count is also satisfied by
            // non-voting members (a set larger than seven has them), and only a majority of
            // the voters is the commit point.
            Document writeConcern = new Document("w", wc >= majority ? "majority" : (Object) wc);
            if (!serverHooks) {
                writeConcern.append("wtimeout", maxTimeMs);
            }
            command.append("writeConcern", writeConcern);
        }
        return command;
    }

    // ===== Reply translation =====

    private static long timestampValue(Document envelope, String field) {
        BsonTimestamp ts = envelope.get(field, BsonTimestamp.class);
        if (ts == null) {
            throw new IllegalStateException("$chameleon reply envelope lacks timestamp field '" + field + "'");
        }
        return ts.getValue();
    }

    private static Document envelopeOf(Document reply) {
        Document envelope = reply.get(ENVELOPE_FIELD, Document.class);
        if (envelope == null) {
            throw new IllegalStateException("reply lacks the " + ENVELOPE_FIELD
                    + " envelope; the server is not the patched mongod (or chameleon is disabled)");
        }
        return envelope;
    }

    /**
     * Translate a successful command reply. With the server hooks on, every graded
     * quantity comes from the $chameleon envelope; without them (stock semantics)
     * the reply's operationTime stands in for the read timestamp and the write's
     * opTime, the requested level and concern are what was delivered, and the
     * quantities stock does not expose are explicitly unknown: commit point -1,
     * service time 0.
     */
    static KvResponse toResponse(KvRequest request, Document reply, boolean serverHooks, int majority) {
        if (!serverHooks && reply.get(ENVELOPE_FIELD) == null) {
            return stockResponse(request, reply, majority);
        }
        Document env = envelopeOf(reply);
        long lastApplied = timestampValue(env, "lastApplied");
        long commit = timestampValue(env, "commit");
        KvResponse.Builder response = KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(true)
                .setLogIndex(lastApplied)
                .setCommitIndex(commit)
                .setServiceTimeMs(env.getDouble("serviceMs"))
                .setPredictedProfit(env.get("predicted") == null ? 0.0 : env.getDouble("predicted"))
                .setWaited(env.getBoolean("waited", false))
                .setTimedOutAndFellBack(env.getBoolean("fellBack", false))
                .setServerReplyEpochMs(env.getLong("epochMs"));
        if (request.getIsRead()) {
            long readTs = timestampValue(env, "readTs");
            ReadLevel level = ReadLevel.forNumber(env.getInteger("level"));
            if (level == null) {
                throw new IllegalStateException("unknown delivered read level " + env.getInteger("level"));
            }
            response.setDeliveredReadLevel(level)
                    .setValueIndex(readTs)
                    .setLocalValueIndex(readTs)
                    // Everything at or below the commit point is majority-committed state.
                    .setCommittedValueIndex(readTs <= commit ? readTs : -1);
        } else {
            response.setValueIndex(timestampValue(env, "entryTs"))
                    .setDeliveredWriteConcern(env.getInteger("wc"));
            if (reply.get("writeConcernError") != null) {
                response.setTimedOutAndFellBack(true);
            }
        }
        return response.build();
    }

    private static KvResponse stockResponse(KvRequest request, Document reply, int majority) {
        BsonTimestamp operationTime = reply.get("operationTime", BsonTimestamp.class);
        if (operationTime == null) {
            throw new IllegalStateException("reply lacks operationTime; stock grading (mongodb.serverHooks: false)"
                    + " needs a replica set reply");
        }
        long opTime = operationTime.getValue();
        KvResponse.Builder response = KvResponse.newBuilder()
                .setRequestId(request.getRequestId())
                .setOk(true)
                // operationTime on a reply is the node's last applied opTime at the read, or the write's opTime.
                .setLogIndex(opTime)
                .setCommitIndex(-1)
                .setServiceTimeMs(0.0)
                .setPredictedProfit(0.0)
                .setWaited(false)
                .setTimedOutAndFellBack(false)
                .setServerReplyEpochMs(0);
        if (request.getIsRead()) {
            ReadLevel delivered = request.hasTargetReadLevel() ? request.getTargetReadLevel()
                    : request.getWantLinearizable() ? ReadLevel.LINEARIZABLE
                    // No readConcern on the wire: the server default is local.
                    : ReadLevel.EVENTUAL_LOCAL;
            response.setDeliveredReadLevel(delivered)
                    .setValueIndex(opTime)
                    .setLocalValueIndex(opTime)
                    .setCommittedValueIndex(-1);
        } else {
            int wc = request.getRequestedWriteConcern();
            // No writeConcern on the wire: a replica set without arbiters defaults to majority.
            response.setValueIndex(opTime)
                    .setDeliveredWriteConcern(wc > 0 ? wc : majority);
            if (reply.get("writeConcernError") != null) {
                response.setTimedOutAndFellBack(true);
            }
        }
        return response.build();
    }

    private int nodeIdOf(ServerAddress address) {
        for (int node = 0; node < settings.hosts().size(); node++) {
            if (address.getPort() == settings.port() && address.getHost().equals(settings.hosts().get(node))) {
                return node;
            }
        }
        return -1;
    }

    /** The primary in the topology view of the client that carried the request. */
    private int leaderHint(int siteId, int client) {
        for (ServerDescription server : siteClients[siteId][client].getClusterDescription().getServerDescriptions()) {
            if (server.isPrimary()) {
                return nodeIdOf(server.getAddress());
            }
        }
        return -1;
    }

    /**
     * The server stopped the operation at its maxTimeMS, which the transport set to the
     * request's remaining SLA deadline. The driver raises this as MongoExecutionTimeoutException,
     * a MongoException that is not a MongoCommandException, so it needs its own branch.
     */
    static KvResponse deadlineExceededReply(KvRequest request) {
        return KvResponse.newBuilder().setRequestId(request.getRequestId()).setOk(false)
                .setDeadlineExceeded(true).build();
    }

    private KvResponse errorReply(int siteId, int client, KvRequest request, MongoCommandException e) {
        KvResponse.Builder response = KvResponse.newBuilder().setRequestId(request.getRequestId()).setOk(false);
        BsonDocument raw = e.getResponse();
        BsonValue envelopeValue = raw == null ? null : raw.get(ENVELOPE_FIELD);
        if (envelopeValue != null && envelopeValue.isDocument()) {
            BsonDocument env = envelopeValue.asDocument();
            if (env.containsKey("serviceMs")) {
                response.setServiceTimeMs(env.getNumber("serviceMs").doubleValue());
            }
            if (env.containsKey("epochMs")) {
                response.setServerReplyEpochMs(env.getNumber("epochMs").longValue());
            }
        }
        if (REJECTED_CODE_NAME.equals(e.getErrorCodeName())) {
            return response.setRejected(true).build();
        }
        switch (e.getErrorCode()) {
            case NOT_WRITABLE_PRIMARY, NOT_PRIMARY_NO_SECONDARY_OK, NOT_PRIMARY_OR_SECONDARY -> {
                return response.setNotLeader(true).setLeaderId(leaderHint(siteId, client)).build();
            }
            case MAX_TIME_MS_EXPIRED -> {
                return deadlineExceededReply(request);
            }
            default -> {
                return null;
            }
        }
    }

    private final class CommandSubscriber implements Subscriber<Document> {
        private final int siteId;
        private final int client;
        private final KvRequest request;
        private final Consumer<KvResponse> onResponse;
        private final Consumer<RpcFailure> onError;
        private final AtomicBoolean settled = new AtomicBoolean();
        private volatile Subscription subscription;
        private volatile Document reply;

        CommandSubscriber(int siteId, int client, KvRequest request, Consumer<KvResponse> onResponse,
                Consumer<RpcFailure> onError) {
            this.siteId = siteId;
            this.client = client;
            this.request = request;
            this.onResponse = onResponse;
            this.onError = onError;
        }

        void cancel() {
            Subscription s = subscription;
            if (s != null && settled.compareAndSet(false, true)) {
                s.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = s;
            s.request(1);
        }

        @Override
        public void onNext(Document document) {
            reply = document;
        }

        @Override
        public void onError(Throwable t) {
            if (!settled.compareAndSet(false, true)) {
                return;
            }
            if (t instanceof MongoExecutionTimeoutException) {
                onResponse.accept(deadlineExceededReply(request));
                return;
            }
            if (t instanceof MongoCommandException commandException) {
                KvResponse mapped;
                try {
                    mapped = errorReply(siteId, client, request, commandException);
                } catch (RuntimeException translation) {
                    onError.accept(new RpcFailure(Status.INTERNAL.withDescription(translation.toString()), 0));
                    return;
                }
                if (mapped != null) {
                    onResponse.accept(mapped);
                    return;
                }
                onError.accept(new RpcFailure(Status.UNAVAILABLE.withDescription(
                        "mongod error " + commandException.getErrorCodeName() + ": "
                                + commandException.getErrorMessage()), 0));
                return;
            }
            // A checkout that timed out is the same phenomenon as the pre-dispatch refusal:
            // our own pool could not produce a connection. Retry behaviour is unchanged;
            // only the outcome it is finally counted under.
            Cause cause = t instanceof MongoTimeoutException && t.getMessage() != null
                    && t.getMessage().contains("while waiting for a connection")
                            ? Cause.TRANSPORT_QUEUE : Cause.SERVER;
            onError.accept(new RpcFailure(Status.UNAVAILABLE.withDescription(describe(t)), 0, cause));
        }

        /** The exception and its cause chain: "Exception opening socket" alone says nothing. */
        private static String describe(Throwable t) {
            StringBuilder sb = new StringBuilder(t.toString());
            for (Throwable c = t.getCause(); c != null && sb.length() < 1000; c = c.getCause()) {
                sb.append(" <- ").append(c);
            }
            return sb.toString();
        }

        @Override
        public void onComplete() {
            if (!settled.compareAndSet(false, true)) {
                return;
            }
            Document document = reply;
            if (document == null) {
                onError.accept(new RpcFailure(Status.INTERNAL.withDescription("runCommand completed without a reply"), 0));
                return;
            }
            KvResponse response;
            try {
                response = toResponse(request, document, settings.serverHooks(), settings.majority());
            } catch (RuntimeException translation) {
                onError.accept(new RpcFailure(Status.INTERNAL.withDescription(translation.toString()), 0));
                return;
            }
            onResponse.accept(response);
        }
    }

    // ===== Per-site source address binding =====

    /**
     * Netty socket channel that connects from a fixed local address. Netty
     * instantiates the channel class reflectively with no arguments, so the
     * site is baked into a subclass and the address table is static.
     */
    public abstract static class SiteBoundChannel extends NioSocketChannel {
        /**
         * One subclass per possible site, below. 32 covers the 27-server scale configs
         * (one client site per server) with room to spare; add classes to raise it.
         */
        static final int MAX_SITES = 32;
        private static volatile String[] bindHosts = new String[0];

        static void install(List<String> hosts) {
            bindHosts = hosts.toArray(new String[0]);
        }

        @SuppressWarnings("unchecked")
        static Class<? extends SocketChannel> classForSite(int site) {
            Class<?>[] classes = {
                    Site0.class, Site1.class, Site2.class, Site3.class, Site4.class, Site5.class,
                    Site6.class, Site7.class, Site8.class, Site9.class, Site10.class, Site11.class,
                    Site12.class, Site13.class, Site14.class, Site15.class, Site16.class, Site17.class,
                    Site18.class, Site19.class, Site20.class, Site21.class, Site22.class, Site23.class,
                    Site24.class, Site25.class, Site26.class, Site27.class, Site28.class, Site29.class,
                    Site30.class, Site31.class,
            };
            if (classes.length != MAX_SITES) {
                throw new IllegalStateException("site channel table has " + classes.length + " classes, MAX_SITES is " + MAX_SITES);
            }
            return (Class<? extends SocketChannel>) classes[site];
        }

        protected abstract int site();

        @Override
        protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress) throws Exception {
            String[] hosts = bindHosts;
            String host = site() < hosts.length ? hosts[site()] : null;
            return super.doConnect(remoteAddress, host == null ? localAddress : new InetSocketAddress(host, 0));
        }

        public static final class Site0 extends SiteBoundChannel { @Override protected int site() { return 0; } }
        public static final class Site1 extends SiteBoundChannel { @Override protected int site() { return 1; } }
        public static final class Site2 extends SiteBoundChannel { @Override protected int site() { return 2; } }
        public static final class Site3 extends SiteBoundChannel { @Override protected int site() { return 3; } }
        public static final class Site4 extends SiteBoundChannel { @Override protected int site() { return 4; } }
        public static final class Site5 extends SiteBoundChannel { @Override protected int site() { return 5; } }
        public static final class Site6 extends SiteBoundChannel { @Override protected int site() { return 6; } }
        public static final class Site7 extends SiteBoundChannel { @Override protected int site() { return 7; } }
        public static final class Site8 extends SiteBoundChannel { @Override protected int site() { return 8; } }
        public static final class Site9 extends SiteBoundChannel { @Override protected int site() { return 9; } }
        public static final class Site10 extends SiteBoundChannel { @Override protected int site() { return 10; } }
        public static final class Site11 extends SiteBoundChannel { @Override protected int site() { return 11; } }
        public static final class Site12 extends SiteBoundChannel { @Override protected int site() { return 12; } }
        public static final class Site13 extends SiteBoundChannel { @Override protected int site() { return 13; } }
        public static final class Site14 extends SiteBoundChannel { @Override protected int site() { return 14; } }
        public static final class Site15 extends SiteBoundChannel { @Override protected int site() { return 15; } }
        public static final class Site16 extends SiteBoundChannel { @Override protected int site() { return 16; } }
        public static final class Site17 extends SiteBoundChannel { @Override protected int site() { return 17; } }
        public static final class Site18 extends SiteBoundChannel { @Override protected int site() { return 18; } }
        public static final class Site19 extends SiteBoundChannel { @Override protected int site() { return 19; } }
        public static final class Site20 extends SiteBoundChannel { @Override protected int site() { return 20; } }
        public static final class Site21 extends SiteBoundChannel { @Override protected int site() { return 21; } }
        public static final class Site22 extends SiteBoundChannel { @Override protected int site() { return 22; } }
        public static final class Site23 extends SiteBoundChannel { @Override protected int site() { return 23; } }
        public static final class Site24 extends SiteBoundChannel { @Override protected int site() { return 24; } }
        public static final class Site25 extends SiteBoundChannel { @Override protected int site() { return 25; } }
        public static final class Site26 extends SiteBoundChannel { @Override protected int site() { return 26; } }
        public static final class Site27 extends SiteBoundChannel { @Override protected int site() { return 27; } }
        public static final class Site28 extends SiteBoundChannel { @Override protected int site() { return 28; } }
        public static final class Site29 extends SiteBoundChannel { @Override protected int site() { return 29; } }
        public static final class Site30 extends SiteBoundChannel { @Override protected int site() { return 30; } }
        public static final class Site31 extends SiteBoundChannel { @Override protected int site() { return 31; } }
    }
}
