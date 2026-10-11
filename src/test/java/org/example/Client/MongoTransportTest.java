package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.TransportSettings;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

/**
 * The MongoDB transport's request and reply translation, without a server:
 * the envelope in the comment field, the readConcern a client-decided target
 * maps to, and how the $chameleon reply envelope becomes a KvResponse the
 * grader understands.
 */
class MongoTransportTest {

    private static KvRequest.Builder read(long frontier) {
        return KvRequest.newBuilder().setRequestId(7).setApplicationId(1).setSlaId(2).setIsRead(true)
                .setKey("user42").setSessionIndex(frontier).setRttEstimateMs(12.5);
    }

    @Test
    void envelopeCarriesIdentityFrontierAndTarget() {
        KvRequest request = read(new BsonTimestamp(1700000000, 9).getValue())
                .setTargetReadLevel(ReadLevel.CAUSAL_MAJORITY).setWaitBoundMs(80).build();
        Document env = MongoTransport.envelope(request, 0.0);
        assertEquals(1, env.getInteger("app"));
        assertEquals(2, env.getInteger("sla"));
        assertEquals(12.5, env.getDouble("rtt"));
        assertTrue(env.getBoolean("read"));
        assertEquals(new BsonTimestamp(1700000000, 9), env.get("frontier", BsonTimestamp.class));
        assertEquals(ReadLevel.CAUSAL_MAJORITY.getNumber(), env.getInteger("target"));
        assertEquals(80.0, env.getDouble("waitBound"));
    }

    @Test
    void envelopeRttChargesThePoolsCheckoutWait() {
        // The client grades from dispatch, before the pool wait; mongod starts its
        // clock at arrival, after it. The envelope's rtt is what the scorer subtracts
        // from every rung, so the wait the request is about to spend on this pool
        // rides along with the network estimate.
        KvRequest request = read(-1).build();
        assertEquals(12.5, MongoTransport.envelope(request, 0.0).getDouble("rtt"));
        assertEquals(100.0, MongoTransport.envelope(request, 87.5).getDouble("rtt"));
        assertThrows(IllegalArgumentException.class, () -> MongoTransport.envelope(request, -1.0));
        assertThrows(IllegalArgumentException.class, () -> MongoTransport.envelope(request, Double.NaN));
    }

    @Test
    void noFrontierAndNoTargetAreOmitted() {
        Document env = MongoTransport.envelope(read(-1).build(), 0.0);
        assertFalse(env.containsKey("frontier"));
        assertFalse(env.containsKey("target"));
    }

    @Test
    void readConcernFollowsTheTargetLevel() {
        long frontier = new BsonTimestamp(5, 1).getValue();
        assertNull(MongoTransport.readConcernFor(read(frontier).build()), "server decides when untargeted");
        assertEquals(new Document("level", "linearizable"),
                MongoTransport.readConcernFor(read(frontier).setWantLinearizable(true).build()));
        assertEquals(new Document("level", "local"),
                MongoTransport.readConcernFor(read(frontier).setTargetReadLevel(ReadLevel.EVENTUAL_LOCAL).build()));
        assertEquals(new Document("level", "majority"),
                MongoTransport.readConcernFor(read(frontier).setTargetReadLevel(ReadLevel.EVENTUAL_MAJORITY).build()));
        assertEquals(new Document("level", "local").append("afterClusterTime", new BsonTimestamp(5, 1)),
                MongoTransport.readConcernFor(read(frontier).setTargetReadLevel(ReadLevel.CAUSAL_LOCAL).build()));
        assertEquals(new Document("level", "majority").append("afterClusterTime", new BsonTimestamp(5, 1)),
                MongoTransport.readConcernFor(read(frontier).setTargetReadLevel(ReadLevel.CAUSAL_MAJORITY).build()));
        assertEquals(new Document("level", "local"),
                MongoTransport.readConcernFor(read(-1).setTargetReadLevel(ReadLevel.CAUSAL_LOCAL).build()),
                "a causal target with nothing observed yet has no afterClusterTime");
    }

    @Test
    void readReplyMapsTimestampsToIndices() {
        KvRequest request = read(-1).build();
        BsonTimestamp commit = new BsonTimestamp(100, 5);
        BsonTimestamp lastApplied = new BsonTimestamp(100, 9);
        Document reply = new Document("ok", 1.0).append("$chameleon", new Document("level", 2)
                .append("wc", 0).append("lastApplied", lastApplied).append("commit", commit)
                .append("readTs", new BsonTimestamp(100, 7)).append("serviceMs", 3.25).append("predicted", 4.5)
                .append("waited", true).append("fellBack", false).append("rejected", false).append("epochMs", 1234L));
        KvResponse response = MongoTransport.toResponse(request, reply, true, 3);
        assertTrue(response.getOk());
        assertEquals(7, response.getRequestId());
        assertEquals(ReadLevel.CAUSAL_LOCAL, response.getDeliveredReadLevel());
        assertEquals(lastApplied.getValue(), response.getLogIndex());
        assertEquals(commit.getValue(), response.getCommitIndex());
        assertEquals(new BsonTimestamp(100, 7).getValue(), response.getValueIndex());
        assertEquals(new BsonTimestamp(100, 7).getValue(), response.getLocalValueIndex());
        assertEquals(-1, response.getCommittedValueIndex(), "a read past the commit point has no committed view");
        assertEquals(3.25, response.getServiceTimeMs());
        assertEquals(4.5, response.getPredictedProfit());
        assertTrue(response.getWaited());
        assertEquals(1234L, response.getServerReplyEpochMs());

        Document committedReply = new Document(reply);
        committedReply.put("$chameleon", new Document(reply.get("$chameleon", Document.class))
                .append("readTs", new BsonTimestamp(100, 3)));
        KvResponse committed = MongoTransport.toResponse(request, committedReply, true, 3);
        assertEquals(new BsonTimestamp(100, 3).getValue(), committed.getCommittedValueIndex(),
                "a read at or below the commit point is majority-committed state");
    }

    @Test
    void writeReplyCarriesEntryTimestampAndConcern() {
        KvRequest request = KvRequest.newBuilder().setRequestId(9).setApplicationId(1).setSlaId(1).setIsRead(false)
                .setKey("user1").setValue("v").setRequestedWriteConcern(3).build();
        Document reply = new Document("ok", 1.0).append("n", 1).append("$chameleon", new Document("level", -1)
                .append("wc", 3).append("lastApplied", new BsonTimestamp(200, 1)).append("commit", new BsonTimestamp(199, 8))
                .append("entryTs", new BsonTimestamp(200, 1)).append("serviceMs", 21.0).append("predicted", 0.0)
                .append("waited", true).append("fellBack", false).append("rejected", false).append("epochMs", 1L));
        KvResponse response = MongoTransport.toResponse(request, reply, true, 3);
        assertEquals(new BsonTimestamp(200, 1).getValue(), response.getValueIndex());
        assertEquals(3, response.getDeliveredWriteConcern());
        assertFalse(response.getTimedOutAndFellBack());

        Document timedOut = new Document(reply).append("writeConcernError", new Document("code", 64));
        assertTrue(MongoTransport.toResponse(request, timedOut, true, 3).getTimedOutAndFellBack());
    }

    @Test
    void replyWithoutEnvelopeIsAnExplicitErrorWhenHooksAreOn() {
        Document stock = new Document("ok", 1.0).append("cursor", new Document("firstBatch", List.of()))
                .append("operationTime", new BsonTimestamp(100, 1));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> MongoTransport.toResponse(read(-1).build(), stock, true, 3));
        assertTrue(e.getMessage().contains("$chameleon"));
    }

    @Test
    void stockReadReplyGradesFromOperationTimeAndTheRequestedLevel() {
        BsonTimestamp opTime = new BsonTimestamp(100, 4);
        Document stock = new Document("ok", 1.0).append("cursor", new Document("firstBatch", List.of()))
                .append("operationTime", opTime);
        KvResponse targeted = MongoTransport.toResponse(
                read(-1).setTargetReadLevel(ReadLevel.CAUSAL_LOCAL).build(), stock, false, 3);
        assertTrue(targeted.getOk());
        assertEquals(ReadLevel.CAUSAL_LOCAL, targeted.getDeliveredReadLevel(), "the level on the wire is what was delivered");
        assertEquals(opTime.getValue(), targeted.getValueIndex());
        assertEquals(opTime.getValue(), targeted.getLocalValueIndex());
        assertEquals(opTime.getValue(), targeted.getLogIndex());
        assertEquals(-1, targeted.getCommitIndex(), "stock replies do not carry the commit point");
        assertEquals(-1, targeted.getCommittedValueIndex());
        assertEquals(0.0, targeted.getServiceTimeMs(), "stock replies carry no server-side timing");
        assertFalse(targeted.getWaited());

        assertEquals(ReadLevel.LINEARIZABLE,
                MongoTransport.toResponse(read(-1).setWantLinearizable(true).build(), stock, false, 3)
                        .getDeliveredReadLevel());
        assertEquals(ReadLevel.EVENTUAL_LOCAL,
                MongoTransport.toResponse(read(-1).build(), stock, false, 3).getDeliveredReadLevel(),
                "no readConcern on the wire means the server default, local");

        Document withEnvelope = new Document(stock).append("$chameleon", new Document("level", 2)
                .append("wc", 0).append("lastApplied", opTime).append("commit", opTime).append("readTs", opTime)
                .append("serviceMs", 1.0).append("waited", false).append("fellBack", false).append("epochMs", 5L));
        assertEquals(1.0, MongoTransport.toResponse(read(-1).build(), withEnvelope, false, 3).getServiceTimeMs(),
                "an envelope that is present is used even with the hooks configured off");
    }

    @Test
    void stockWriteReplyGradesFromOperationTimeAndTheRequestedConcern() {
        BsonTimestamp opTime = new BsonTimestamp(200, 2);
        KvRequest.Builder write = KvRequest.newBuilder().setRequestId(9).setApplicationId(1).setSlaId(1)
                .setIsRead(false).setKey("user1").setValue("v");
        Document stock = new Document("ok", 1.0).append("n", 1).append("nModified", 1).append("operationTime", opTime);
        KvResponse response = MongoTransport.toResponse(write.setRequestedWriteConcern(1).build(), stock, false, 3);
        assertEquals(opTime.getValue(), response.getValueIndex(), "operationTime on a write reply is the write's opTime");
        assertEquals(1, response.getDeliveredWriteConcern());
        assertFalse(response.getTimedOutAndFellBack());
        assertEquals(3, MongoTransport.toResponse(write.setRequestedWriteConcern(0).build(), stock, false, 3)
                .getDeliveredWriteConcern(), "no writeConcern on the wire means the replica set default, majority");
        Document timedOut = new Document(stock).append("writeConcernError", new Document("code", 64));
        assertTrue(MongoTransport.toResponse(write.setRequestedWriteConcern(3).build(), timedOut, false, 3)
                .getTimedOutAndFellBack());
    }

    @Test
    void stockReplyWithoutOperationTimeIsAnExplicitError() {
        Document standalone = new Document("ok", 1.0).append("cursor", new Document("firstBatch", List.of()));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> MongoTransport.toResponse(read(-1).build(), standalone, false, 3));
        assertTrue(e.getMessage().contains("operationTime"));
    }

    @Test
    void writeCommandBoundsTheAcknowledgmentWaitWithoutTrippingTheDriver() {
        KvRequest write = KvRequest.newBuilder().setRequestId(9).setApplicationId(1).setSlaId(1).setIsRead(false)
                .setKey("user1").setValue("v").setRequestedWriteConcern(2).build();
        Document hooksOn = MongoTransport.updateCommand(write, 750, 0.0, "kv", true, 3);
        assertFalse(hooksOn.containsKey("maxTimeMS"),
                "a write never carries maxTimeMS: its expiry inside the replication wait marks the primary UNKNOWN");
        assertEquals(new Document("w", 2), hooksOn.get("writeConcern", Document.class),
                "with the hooks on the server bounds the wait from the envelope");

        Document hooksOff = MongoTransport.updateCommand(write, 750, 0.0, "kv", false, 3);
        assertFalse(hooksOff.containsKey("maxTimeMS"), "without the hooks a write never carries maxTimeMS");
        assertEquals(new Document("w", 2).append("wtimeout", 750L), hooksOff.get("writeConcern", Document.class),
                "the remaining deadline bounds the acknowledgment wait instead");
        assertEquals("kv", hooksOff.getString("update"));
        assertTrue(hooksOff.containsKey("comment"), "the envelope still travels for the hook-cost comparison");
    }

    @Test
    void aWriteAtTheMajorityGoesAsMajorityNotAsACount() {
        KvRequest write = KvRequest.newBuilder().setRequestId(9).setApplicationId(1).setSlaId(1).setIsRead(false)
                .setKey("user1").setValue("v").setRequestedWriteConcern(3).build();
        assertEquals(new Document("w", "majority"),
                MongoTransport.updateCommand(write, 750, 0.0, "kv", true, 3).get("writeConcern", Document.class),
                "a count is also satisfied by non-voting members; only a majority of the voters is the commit point");
        assertEquals(new Document("w", "majority").append("wtimeout", 750L),
                MongoTransport.updateCommand(write, 750, 0.0, "kv", false, 3).get("writeConcern", Document.class));
        assertEquals(new Document("w", 3),
                MongoTransport.updateCommand(write, 750, 0.0, "kv", true, 4).get("writeConcern", Document.class),
                "below the majority the count stands");
    }

    @Test
    void theMajorityIsAMajorityOfTheVotingMembers() {
        assertEquals(2, settingsFor(3).majority());
        assertEquals(3, settingsFor(5).majority());
        assertEquals(4, settingsFor(7).majority());
        assertEquals(4, settingsFor(27).majority(),
                "MongoDB allows seven voters; the other twenty members replicate without a vote");
    }

    @Test
    void oneBoundChannelClassPerSiteUpToTheCap() throws Exception {
        assertTrue(MongoTransport.SiteBoundChannel.MAX_SITES >= 27, "the 27-server scale configs need 27 client sites");
        java.util.Set<Class<?>> seen = new java.util.HashSet<>();
        for (int site = 0; site < MongoTransport.SiteBoundChannel.MAX_SITES; site++) {
            Class<?> c = MongoTransport.SiteBoundChannel.classForSite(site);
            assertTrue(seen.add(c), "site " + site + " reuses " + c.getSimpleName());
            assertEquals("Site" + site, c.getSimpleName(), "the table must be in site order");
            assertTrue(MongoTransport.SiteBoundChannel.class.isAssignableFrom(c));
        }
        assertThrows(ArrayIndexOutOfBoundsException.class,
                () -> MongoTransport.SiteBoundChannel.classForSite(MongoTransport.SiteBoundChannel.MAX_SITES));
    }

    private static MongoTransport.Settings settingsFor(int hosts) {
        List<String> names = new java.util.ArrayList<>();
        for (int i = 0; i < hosts; i++) {
            names.add("h" + i);
        }
        return new MongoTransport.Settings(names, 27017, "rs0", "db", "kv", 1000, 200, 1000, List.of("127.0.2.1"),
                List.of("virginia"), true);
    }

    @Test
    void poolBudgetSplitsOverTheSiteClientsRoundingUp() {
        MongoTransport.Settings settings = new MongoTransport.Settings(List.of("h0", "h1", "h2"), 27017, "rs0",
                "db", "kv", 1667, 200, 1000, List.of("127.0.2.1"), List.of("virginia"), true);
        int share = settings.poolSizePerClient();
        assertTrue(share * MongoTransport.CLIENTS_PER_SITE >= 1667, "the clients together never hold less than the budget");
        assertTrue((share - 1) * MongoTransport.CLIENTS_PER_SITE < 1667, "and no more than one connection each above it");
    }

    @Test
    void poolGrowsUnthrottledAndWaitsItsOwnTimeout() {
        MongoTransport.Settings settings = new MongoTransport.Settings(List.of("h0", "h1", "h2"), 27017, "rs0",
                "db", "kv", 4000, 200, 1000, List.of("127.0.2.1"), List.of("virginia"), true);
        MongoDriverEvents events = new MongoDriverEvents("virginia");
        MongoClientSettings client = MongoTransport.clientSettings(settings,
                List.of(new ServerAddress("h0", 27017)), TransportSettings.nettyBuilder().build(), events);
        assertTrue(client.getServerSettings().getServerListeners().contains(events));
        assertTrue(client.getServerSettings().getServerMonitorListeners().contains(events));
        assertTrue(client.getClusterSettings().getClusterListeners().contains(events));
        assertTrue(client.getConnectionPoolSettings().getConnectionPoolListeners().contains(events));
        assertEquals("", events.summary(), "nothing notable yet");
        assertEquals(4000 / MongoTransport.CLIENTS_PER_SITE, client.getConnectionPoolSettings().getMaxSize(),
                "each of a site's clients holds its share of the pair's connection budget");
        assertEquals(4000 / MongoTransport.CLIENTS_PER_SITE, client.getConnectionPoolSettings().getMaxConnecting(),
                "every missing connection opens at once, not two at a time");
        assertEquals(1000, client.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
        assertEquals(200, client.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS),
                "server selection keeps its own, shorter timeout");
        assertFalse(client.getRetryReads());
        assertFalse(client.getRetryWrites());
        assertEquals(2, settings.majority());
    }

    @Test
    void serverSideDeadlineExpiryIsADeadlineNotAFailure() {
        // The driver raises code 50 as its own MongoException subclass, outside the
        // MongoCommandException mapping; the transport must still grade it as a deadline.
        Throwable expiry = new MongoExecutionTimeoutException(50, "operation exceeded time limit");
        assertFalse(expiry instanceof MongoCommandException);
        KvResponse reply = MongoTransport.deadlineExceededReply(read(0).build());
        assertEquals(7, reply.getRequestId());
        assertFalse(reply.getOk());
        assertTrue(reply.getDeadlineExceeded());
        assertFalse(reply.getRejected());
    }

    @Test
    void aTransportThatDoesNotQueueReportsNoQueueWait() {
        // The framed transport multiplexes over pre-opened sockets and accepts a write
        // without waiting, so it inherits this default and the session client subtracts
        // nothing. Only the pooled MongoDB transport overrides it.
        KvTransport noQueue = new KvTransport() {
            @Override
            public int numServers() {
                return 5;
            }

            @Override
            public int numSites() {
                return 6;
            }

            @Override
            public RequestHandle execute(int siteId, int nodeId, KvRequest request, long remainingDeadlineNanos,
                    java.util.function.Consumer<KvResponse> onResponse,
                    java.util.function.Consumer<RpcFailure> onError) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
            }
        };
        assertEquals(0.0, noQueue.queueWaitMs(3, 2));
    }

    @Test
    void aQueueRefusalIsItsOwnCauseAndAServerErrorIsNot() {
        // The session client counts a TRANSPORT_QUEUE failure apart from violations and
        // from admission rejections, so the two constructors must not be interchangeable.
        KvTransport.RpcFailure server = new KvTransport.RpcFailure(
                io.grpc.Status.UNAVAILABLE.withDescription("node down"), 0);
        assertEquals(KvTransport.Cause.SERVER, server.cause(), "the default stays server-attributed");

        KvTransport.RpcFailure queued = new KvTransport.RpcFailure(
                io.grpc.Status.DEADLINE_EXCEEDED.withDescription("pool full"), 0,
                KvTransport.Cause.TRANSPORT_QUEUE);
        assertEquals(KvTransport.Cause.TRANSPORT_QUEUE, queued.cause());
    }
}
