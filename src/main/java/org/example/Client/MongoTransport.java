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
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoExecutionTimeoutException;
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
 * (MONGODB_PORT.md, Milestone 3). One MongoClient per client site, sharing
 * one Netty event loop; the site's source address is bound inside the Netty
 * socket channel so the netem delay rules can match per (site, server) pair
 * without a process or namespace per site.
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
 * {app, sla, rtt, read, frontier?, target?, wantLin, wc, waitBound}.
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

        /** Acknowledgments a replica set of this size needs for w:majority. */
        int majority() {
            return hosts.size() / 2 + 1;
        }
    }

    private final Settings settings;
    private final EventLoopGroup eventLoopGroup;
    private final MongoClient[] siteClients;
    private final MongoDriverEvents[] siteEvents;
    private final MongoDatabase[] siteDatabases;
    private final ReadPreference[] nodePreferences;
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
        this.siteClients = new MongoClient[numSites];
        this.siteDatabases = new MongoDatabase[numSites];
        this.siteEvents = new MongoDriverEvents[numSites];
        for (int site = 0; site < numSites; site++) {
            siteEvents[site] = new MongoDriverEvents(settings.siteNames().get(site));
            MongoClientSettings clientSettings = clientSettings(settings, seeds, TransportSettings.nettyBuilder()
                    .eventLoopGroup(eventLoopGroup)
                    .socketChannelClass(SiteBoundChannel.classForSite(site))
                    .build(), siteEvents[site]);
            siteClients[site] = MongoClients.create(clientSettings);
            siteDatabases[site] = siteClients[site].getDatabase(settings.database());
        }
        this.nodePreferences = new ReadPreference[settings.hosts().size()];
        for (int node = 0; node < nodePreferences.length; node++) {
            nodePreferences[node] = ReadPreference.nearest(new TagSet(new Tag("node", Integer.toString(node))));
        }
    }

    /** Driver settings for one site's client; the transport is the site's bound Netty channel. */
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
                .applyToConnectionPoolSettings(b -> b.maxSize(settings.maxPoolSize())
                        .maxConnecting(settings.maxPoolSize())
                        .maxWaitTime(settings.poolWaitTimeoutMs(), TimeUnit.MILLISECONDS)
                        .addConnectionPoolListener(events))
                .transportSettings(transport)
                // The harness owns retries: a driver retry would hide redirects and double count.
                .retryReads(false)
                .retryWrites(false)
                .build();
    }

    @Override
    public int numServers() {
        return settings.hosts().size();
    }

    @Override
    public int numSites() {
        return siteClients.length;
    }

    @Override
    public RequestHandle execute(int siteId, int nodeId, KvRequest request, long remainingDeadlineNanos,
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
        Document command = request.getIsRead() ? findCommand(request, maxTimeMs) : updateCommand(request, maxTimeMs);
        Publisher<Document> publisher = siteDatabases[siteId].runCommand(command, nodePreferences[nodeId]);

        CommandSubscriber subscriber = new CommandSubscriber(siteId, request, onResponse, onError);
        publisher.subscribe(subscriber);
        return subscriber::cancel;
    }

    /** What the driver did to its cluster view during the run, per site; empty when nothing notable. */
    public String driverEventSummary() {
        StringBuilder sb = new StringBuilder();
        for (MongoDriverEvents events : siteEvents) {
            sb.append(events.summary());
        }
        return sb.toString();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (MongoClient client : siteClients) {
            client.close();
        }
        eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }

    // ===== Request translation =====

    /** Frontier as a BSON timestamp; null when the session has observed nothing (-1). */
    static BsonTimestamp frontierOf(KvRequest request) {
        long frontier = request.getSessionIndex();
        return frontier < 0 ? null : new BsonTimestamp(frontier);
    }

    static Document envelope(KvRequest request) {
        Document env = new Document("app", request.getApplicationId())
                .append("sla", request.getSlaId())
                .append("rtt", request.getRttEstimateMs())
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

    Document findCommand(KvRequest request, long maxTimeMs) {
        Document command = new Document("find", settings.collection())
                .append("filter", new Document("_id", request.getKey()))
                .append("limit", 1)
                .append("singleBatch", true)
                .append("maxTimeMS", maxTimeMs)
                .append("comment", envelope(request));
        Document readConcern = readConcernFor(request);
        if (readConcern != null) {
            command.append("readConcern", readConcern);
        }
        return command;
    }

    Document updateCommand(KvRequest request, long maxTimeMs) {
        return updateCommand(request, maxTimeMs, settings.collection(), settings.serverHooks());
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
    static Document updateCommand(KvRequest request, long maxTimeMs, String collection, boolean serverHooks) {
        Document update = new Document("q", new Document("_id", request.getKey()))
                .append("u", new Document("$set", new Document("v", request.getValue())))
                .append("upsert", true)
                .append("multi", false);
        Document command = new Document("update", collection)
                .append("updates", List.of(update))
                .append("ordered", true);
        command.append("comment", envelope(request));
        int wc = request.getRequestedWriteConcern();
        if (wc > 0) {
            // Client-decided concern; with the hooks on the server clamps it to the
            // majority and bounds the wait by the envelope's waitBound.
            Document writeConcern = new Document("w", wc);
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

    private int leaderHint(int siteId) {
        for (ServerDescription server : siteClients[siteId].getClusterDescription().getServerDescriptions()) {
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

    private KvResponse errorReply(int siteId, KvRequest request, MongoCommandException e) {
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
                return response.setNotLeader(true).setLeaderId(leaderHint(siteId)).build();
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
        private final KvRequest request;
        private final Consumer<KvResponse> onResponse;
        private final Consumer<RpcFailure> onError;
        private final AtomicBoolean settled = new AtomicBoolean();
        private volatile Subscription subscription;
        private volatile Document reply;

        CommandSubscriber(int siteId, KvRequest request, Consumer<KvResponse> onResponse,
                Consumer<RpcFailure> onError) {
            this.siteId = siteId;
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
                    mapped = errorReply(siteId, request, commandException);
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
            onError.accept(new RpcFailure(Status.UNAVAILABLE.withDescription(describe(t)), 0));
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
        static final int MAX_SITES = 8;
        private static volatile String[] bindHosts = new String[0];

        static void install(List<String> hosts) {
            bindHosts = hosts.toArray(new String[0]);
        }

        @SuppressWarnings("unchecked")
        static Class<? extends SocketChannel> classForSite(int site) {
            Class<?>[] classes = { Site0.class, Site1.class, Site2.class, Site3.class,
                    Site4.class, Site5.class, Site6.class, Site7.class };
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
    }
}
