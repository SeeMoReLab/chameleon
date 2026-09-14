package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.example.Client.KvSessionClient;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.example.Utility.RungScorer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Level mechanics through the SLA-driven request path: the scorer chooses the
 * level, so tests steer it with single-rung SLAs. On fresh clusters all
 * histogram cells are empty (free and certain), so ties between levels
 * satisfying the same rungs break toward the weakest level, which makes the
 * chosen level deterministic; assertions that tolerate upgrades say so.
 */
class KvLevelMechanicsTest {

    private static final ReadLevel EL = ReadLevel.EVENTUAL_LOCAL;
    private static final ReadLevel EM = ReadLevel.EVENTUAL_MAJORITY;
    private static final ReadLevel CL = ReadLevel.CAUSAL_LOCAL;
    private static final ReadLevel CM = ReadLevel.CAUSAL_MAJORITY;
    private static final ReadLevel LIN = ReadLevel.LINEARIZABLE;

    private static RungScorer.Rung read(ReadLevel level, double thresholdMs, double profit) {
        return new RungScorer.Rung(level.getNumber(), thresholdMs, profit);
    }

    private static RungScorer.Rung write(int concern, double thresholdMs, double profit) {
        return new RungScorer.Rung(concern, thresholdMs, profit);
    }

    @BeforeAll
    static void economicsDefaults() {
        MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001);
    }

    @Test
    void writeConcernsAndEventualViewsFollowTheSla() throws Exception {
        SlaRegistry.registerWriteSla(10, 1, List.of(write(1, 1000, 5)));
        SlaRegistry.registerWriteSla(11, 1, List.of(write(2, 1000, 5)));
        SlaRegistry.registerReadSla(10, 1, List.of(read(EL, 1000, 5)));
        SlaRegistry.registerReadSla(11, 1, List.of(read(EM, 1000, 5)));

        try (TestCluster cluster = new TestCluster(18400)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {

                // A wc:1 rung is satisfied by both concerns; the tie breaks to
                // the cheaper wc:1, acknowledged at append.
                KvResponse w1 = session.write("k1", "va", 10, 1);
                assertTrue(w1.getOk());
                assertEquals(1, w1.getDeliveredWriteConcern());
                assertFalse(w1.getTimedOutAndFellBack());

                // A wc:2 rung is only satisfiable by wc:2: acknowledged at
                // commit, so the commit index covers the entry.
                KvResponse w2 = session.write("k2", "vb", 11, 1);
                assertTrue(w2.getOk());
                assertEquals(2, w2.getDeliveredWriteConcern());
                assertTrue(w2.getCommitIndex() >= w2.getValueIndex(),
                        "majority ack implies committed: commitIndex=" + w2.getCommitIndex()
                                + " entry=" + w2.getValueIndex());

                // Eventual-local SLA: every level satisfies it, weakest wins.
                KvResponse local = session.read("k1", 10, 1, -1);
                assertEquals("va", local.getValue());
                assertEquals(EL, local.getDeliveredReadLevel());

                // Eventual-majority SLA: EM is the weakest satisfying level.
                KvResponse committed = session.read("k2", 11, 1, -1);
                assertEquals("vb", committed.getValue());
                assertEquals(EM, committed.getDeliveredReadLevel());
            }
        }
    }

    @Test
    void causalMajorityWaitsForCommitOnFollower() throws Exception {
        SlaRegistry.registerWriteSla(12, 1, List.of(write(1, 1000, 5)));
        SlaRegistry.registerReadSla(12, 1, List.of(read(CM, 1000, 5)));

        try (TestCluster cluster = new TestCluster(18500)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

            try (TestSession leaderSession = new TestSession(cluster.portOf(leader.nodeId()));
                    TestSession followerSession = new TestSession(cluster.portOf(follower.nodeId()))) {

                // wc:1 write acknowledged before commit; its index anchors the
                // causal-majority read, which must block until the follower's
                // commit index catches up.
                KvResponse write = leaderSession.write("causal-key", "cv", 12, 1);
                int anchor = write.getValueIndex();

                KvResponse read = followerSession.read("causal-key", 12, 1, anchor);
                assertTrue(read.getOk());
                assertEquals(CM, read.getDeliveredReadLevel(), "only causal-majority satisfies the rung on a follower");
                assertFalse(read.getTimedOutAndFellBack());
                assertEquals("cv", read.getValue());
                assertTrue(read.getValueIndex() >= anchor);
                assertTrue(read.getCommitIndex() >= anchor);
            }
        }
    }

    @Test
    void causalWaitIsBoundedByTheSlaThresholdAndFallsBack() throws Exception {
        // Single causal-local rung with a 50 ms threshold: the wait bound is
        // d_max = 50 ms (not the global maxWaitMs of 400), and on expiry the
        // request falls back to the strongest no-wait level of the same view.
        SlaRegistry.registerReadSla(13, 1, List.of(read(CL, 50, 3)));

        try (TestCluster cluster = new TestCluster(18600)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

            try (TestSession session = new TestSession(cluster.portOf(follower.nodeId()))) {
                int unreachable = follower.lastLogIndex() + 100_000;
                long start = System.nanoTime();
                KvResponse read = session.read("any", 13, 1, unreachable);
                double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;

                assertTrue(read.getOk());
                assertTrue(read.getWaited());
                assertTrue(read.getTimedOutAndFellBack());
                assertEquals(EL, read.getDeliveredReadLevel());
                assertTrue(elapsedMs >= 40, "the bounded wait must actually run, took " + elapsedMs + " ms");
                assertTrue(elapsedMs < 250,
                        "the SLA threshold (50 ms), not maxWaitMs (400 ms), bounds the wait; took " + elapsedMs);
            }
        }
    }

    @Test
    void linearizableOnlySlasRedirectFromFollowersAndServeOnTheLeader() throws Exception {
        SlaRegistry.registerWriteSla(14, 1, List.of(write(2, 1000, 5)));
        SlaRegistry.registerReadSla(14, 1, List.of(read(LIN, 1000, 5)));

        try (TestCluster cluster = new TestCluster(18700)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();

            try (TestSession leaderSession = new TestSession(cluster.portOf(leader.nodeId()));
                    TestSession followerSession = new TestSession(cluster.portOf(follower.nodeId()))) {

                KvResponse write = leaderSession.write("lin-key", "lv", 14, 1);
                assertTrue(write.getOk());

                // No rung is satisfiable by any follower-legal level: the
                // request is illegal here, so redirect rather than reject.
                KvResponse redirected = followerSession.read("lin-key", 14, 1, -1);
                assertFalse(redirected.getOk());
                assertTrue(redirected.getNotLeader());
                assertFalse(redirected.getRejected());
                assertEquals(leader.nodeId(), redirected.getLeaderId());

                KvResponse read = leaderSession.read("lin-key", 14, 1, -1);
                assertTrue(read.getOk());
                assertEquals(LIN, read.getDeliveredReadLevel());
                assertEquals("lv", read.getValue());
                assertTrue(read.getWaited(), "linearizable always waits for the confirmation round");
            }
        }
    }

    @Test
    void followerServesLinearizableReadsWhenEnabled() throws Exception {
        SlaRegistry.registerReadSla(16, 1, List.of(read(LIN, 1000, 5)));
        SlaRegistry.registerWriteSla(16, 1, List.of(write(2, 1000, 5)));

        KvClientService.setFollowerLinearizableReadsForTest(true);
        try (TestCluster cluster = new TestCluster(19600)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
            cluster.awaitLeaderHint(follower, leader, 5_000);

            try (TestSession leaderSession = new TestSession(cluster.portOf(leader.nodeId()));
                    TestSession followerSession = new TestSession(cluster.portOf(follower.nodeId()))) {

                KvResponse write = leaderSession.write("flr-key", "flr-value", 16, 1);
                assertTrue(write.getOk());

                // The LIN-only SLA is now satisfiable on the follower: no
                // redirect. The follower fetches a confirmed read index from
                // the leader, waits for its own commit index, and serves the
                // committed write linearizably.
                KvResponse read = followerSession.read("flr-key", 16, 1, -1);
                assertTrue(read.getOk(), "follower must serve the linearizable read itself");
                assertEquals(LIN, read.getDeliveredReadLevel());
                assertEquals("flr-value", read.getValue());
                assertTrue(read.getWaited(), "the leader round trip counts as waiting");
                assertFalse(read.getTimedOutAndFellBack());
                assertTrue(read.getValueIndex() >= write.getValueIndex(),
                        "linearizable read must observe the committed write");
            }
        } finally {
            KvClientService.setFollowerLinearizableReadsForTest(false);
        }
    }

    @Test
    void readsAdvanceTheSessionFrontierToTheDeliveredValue() throws Exception {
        SlaRegistry.registerReadSla(17, 1, List.of(read(EL, 1000, 5)));
        SlaRegistry.registerWriteSla(17, 1, List.of(write(1, 1000, 5)));

        try (TestCluster cluster = new TestCluster(19900)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            // Another session writes; the session under test has observed
            // nothing yet, so the only way its frontier can reach the write
            // is by reading the value.
            int written;
            try (TestSession writer = new TestSession(cluster.portOf(leader.nodeId()))) {
                KvResponse w = writer.write("frontier-key", "v", 17, 1);
                assertTrue(w.getOk());
                written = w.getValueIndex();
            }
            try (KvSessionClient client = new KvSessionClient(17,
                    List.of("localhost", "localhost", "localhost"), cluster.clientBasePort,
                    org.example.Client.ClientMode.CHAMELEON, 64, 3, 8_000,
                    Map.of(1, List.of(read(EL, 1000, 5))),
                    Map.of(1, List.of(write(1, 1000, 5))),
                    0.0, false, false, 0.9)) {
                assertEquals(-1, client.sessionIndex());

                client.sendRead("never-written", 1);
                awaitQuiescent(client);
                assertEquals(-1, client.sessionIndex(), "an absent key is not an observation");

                // The read may land on a follower that has not replicated the
                // write yet (absent, no advance); retry until the value is
                // delivered somewhere. Once it is, the frontier must equal
                // exactly that value's write index.
                long deadline = System.currentTimeMillis() + 10_000;
                while (client.sessionIndex() < 0 && System.currentTimeMillis() < deadline) {
                    client.sendRead("frontier-key", 1);
                    awaitQuiescent(client);
                }
                assertEquals(written, client.sessionIndex(),
                        "a delivered read makes its value's write a cause of later operations");

                client.sendWrite("frontier-key", "v2", 1);
                awaitQuiescent(client);
                assertTrue(client.sessionIndex() > written, "an acknowledged write advances the frontier");
            }
        }
    }

    private static void awaitQuiescent(KvSessionClient client) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(0, client.pendingCount(), "all requests must resolve");
    }

    @Test
    void sessionClientKeepsReadYourWritesUnderLoad() throws Exception {
        SlaRegistry.registerReadSla(15, 1, List.of(read(CM, 1000, 4), read(CL, 500, 2)));
        SlaRegistry.registerWriteSla(15, 1, List.of(write(2, 1000, 4), write(1, 500, 2)));

        try (TestCluster cluster = new TestCluster(18800)) {
            cluster.awaitLeader(15_000);

            try (KvSessionClient client = new KvSessionClient(15,
                    List.of("localhost", "localhost", "localhost"), cluster.clientBasePort,
                    org.example.Client.ClientMode.CHAMELEON, 64, 3, 8_000,
                    Map.of(1, List.of(read(CM, 1000, 4), read(CL, 500, 2))),
                    Map.of(1, List.of(write(2, 1000, 4), write(1, 500, 2))),
                    0.0, false, false, 0.9)) {

                // Interleave writes and causal reads over a small keyspace so
                // reads constantly chase this session's own writes across all
                // three nodes; the server picks the levels, the assertions key
                // on what was delivered.
                for (int i = 0; i < 300; i++) {
                    String key = "s" + (i % 10);
                    client.sendWrite(key, "v" + i, 1);
                    client.sendRead(key, 1);
                }

                long deadline = System.currentTimeMillis() + 20_000;
                while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                }
                assertEquals(0, client.pendingCount(), "all requests must resolve");
            }
        }
    }
}
