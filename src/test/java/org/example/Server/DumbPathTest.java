package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

/**
 * The non-chameleon server path: the client resolved the subSLA target, the
 * server serves what it has - both views on every read, immediate unless the
 * request explicitly asks for linearizability (ReadIndex round) or a write
 * concern (ack semantics). No scoring, no SLA lookup, no upgrades.
 */
class DumbPathTest {

    @Test
    void plainReadsReturnBothViewsImmediately() throws Exception {
        KvClientService.setChameleonDecisionForTest(false);
        try (TestCluster cluster = new TestCluster(19700)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                // wc:1 write: acked at append, so the local view has it
                // immediately while the committed view may still lag.
                KvResponse write = session.call(
                        TestSession.writeRequest("d-key", "d-value", 60, 1).setRequestedWriteConcern(1));
                assertTrue(write.getOk());
                assertEquals(1, write.getDeliveredWriteConcern());
                assertFalse(write.getWaited());

                KvResponse read = session.read("d-key", 60, 1, -1);
                assertTrue(read.getOk());
                assertFalse(read.getWaited(), "plain dumb reads never wait");
                assertEquals(ReadLevel.EVENTUAL_MAJORITY, read.getDeliveredReadLevel());
                assertEquals("d-value", read.getLocalValue());
                assertTrue(read.getLocalValueIndex() >= write.getValueIndex());
                // Both view fields are populated; the committed pair may
                // trail the local pair but never exceeds the commit index.
                assertTrue(read.getCommittedValueIndex() <= read.getCommitIndex());
                assertEquals(-1, read.getSatisfiedRung(), "the dumb path does not grade");
            }
        } finally {
            KvClientService.setChameleonDecisionForTest(true);
        }
    }

    @Test
    void requestedLinearizabilityRunsReadIndexOnTheLeader() throws Exception {
        KvClientService.setChameleonDecisionForTest(false);
        try (TestCluster cluster = new TestCluster(19750)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                KvResponse write = session.call(
                        TestSession.writeRequest("lin-key", "lin-value", 60, 1)
                                .setRequestedWriteConcern(cluster.nodes.size() / 2 + 1));
                assertTrue(write.getOk());
                assertTrue(write.getWaited(), "majority ack is a wait");
                assertTrue(write.getCommitIndex() >= write.getValueIndex());

                KvResponse read = session.call(
                        TestSession.readRequest("lin-key", 60, 1, -1).setWantLinearizable(true));
                assertTrue(read.getOk());
                assertEquals(ReadLevel.LINEARIZABLE, read.getDeliveredReadLevel());
                assertTrue(read.getWaited(), "the confirmation round counts as waiting");
                assertFalse(read.getTimedOutAndFellBack());
                assertEquals("lin-value", read.getCommittedValue());
            }
        } finally {
            KvClientService.setChameleonDecisionForTest(true);
        }
    }

    @Test
    void requestedLinearizabilityOnFollowersHonorsTheFlag() throws Exception {
        KvClientService.setChameleonDecisionForTest(false);
        try (TestCluster cluster = new TestCluster(19770)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
            cluster.awaitLeaderHint(follower, leader, 5_000);
            try (TestSession leaderSession = new TestSession(cluster.portOf(leader.nodeId()));
                    TestSession followerSession = new TestSession(cluster.portOf(follower.nodeId()))) {
                KvResponse write = leaderSession.call(
                        TestSession.writeRequest("flin-key", "flin-value", 60, 1)
                                .setRequestedWriteConcern(cluster.nodes.size() / 2 + 1));
                assertTrue(write.getOk());

                // Flag off: redirect, same as chameleon mode.
                KvResponse redirected = followerSession.call(
                        TestSession.readRequest("flin-key", 60, 1, -1).setWantLinearizable(true));
                assertFalse(redirected.getOk());
                assertTrue(redirected.getNotLeader());

                // Flag on: the follower serves it via the leader round.
                KvClientService.setFollowerLinearizableReadsForTest(true);
                try {
                    KvResponse served = followerSession.call(
                            TestSession.readRequest("flin-key", 60, 1, -1).setWantLinearizable(true));
                    assertTrue(served.getOk());
                    assertEquals(ReadLevel.LINEARIZABLE, served.getDeliveredReadLevel());
                    assertEquals("flin-value", served.getCommittedValue());
                } finally {
                    KvClientService.setFollowerLinearizableReadsForTest(false);
                }
            }
        } finally {
            KvClientService.setChameleonDecisionForTest(true);
        }
    }
}
