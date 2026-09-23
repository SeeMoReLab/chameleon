package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class ReplicationPipelineTest {

    @Test
    void boundedOrderedPipelineRespectsConfiguredLimitsAndRecoversAStream() throws Exception {
        ServerImpl.applyReplicationSettingsForTest(2, 4);
        try (TestCluster cluster = new TestCluster(19800)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl disconnectedFollower = cluster.nodes.stream()
                    .filter(node -> node != leader)
                    .findFirst()
                    .orElseThrow();

            disconnectedFollower.setDropAllServerNetworkTraffic(true);
            int lastIndex = -1;
            for (int i = 0; i < 100; i++) {
                lastIndex = leader.appendWrite("pipeline-" + (i % 10), "v" + i, "pipeline-op-" + i);
            }
            leader.awaitCommitIndex(lastIndex).get(15, TimeUnit.SECONDS);

            assertTrue(leader.maxObservedReplicationBatchEntries() <= 2,
                    "no stream message may exceed the configured entry cap");
            boolean observedPipelining = false;
            for (ServerImpl follower : cluster.nodes) {
                if (follower != leader) {
                    int maxInflight = leader.maxObservedReplicationInflight(follower.nodeId());
                    assertTrue(maxInflight <= 4,
                            "the speculative per-follower window must remain bounded");
                    observedPipelining |= maxInflight > 1;
                }
            }
            assertTrue(observedPipelining,
                    "the leader should send another batch before the preceding batch is acknowledged");

            disconnectedFollower.setDropAllServerNetworkTraffic(false);
            cluster.awaitCommitted(lastIndex, 15_000);
            assertEquals(lastIndex, disconnectedFollower.currentCommitIndex());
            assertEquals("v99", disconnectedFollower.kv.readCommitted("pipeline-9").value());
        } finally {
            ServerImpl.applyReplicationSettingsForTest(4000, 4);
        }
    }

    /**
     * The pipeline is self-clocked: a follower acknowledgement pumps the next
     * batch at once, so without a linger the batch carries only the entries
     * that arrived during one round trip. On a fast link that is almost
     * nothing, and the leader spends its budget on per-message overhead
     * instead of entries - the reason the local topology batched far worse
     * than the global one for identical work. The linger puts a floor under
     * batch size, so the same writes cost materially fewer messages.
     */
    @Test
    void aLingerBatchesTheSameWritesIntoFewerMessages() throws Exception {
        long withoutLinger = replicationMessagesFor(0.0, 19820);
        long withLinger = replicationMessagesFor(5.0, 19840);
        assertTrue(withLinger * 2 <= withoutLinger,
                "a 5 ms linger should at least halve the replication messages for the same writes, but sent "
                        + withLinger + " against " + withoutLinger);
    }

    /**
     * Messages the leader sent to replicate the same paced writes, at one linger
     * setting. The pacing is the point: a burst all lands in the log before the
     * first pump and leaves in one batch whatever the linger, which is not the
     * regime that goes wrong. Entries have to arrive at a rate comparable to
     * the round trip for the self-clocking to show, exactly as offered load
     * does in a run.
     */
    private static long replicationMessagesFor(double lingerMs, int basePort) throws Exception {
        ServerImpl.applyReplicationSettingsForTest(4000, 16, lingerMs);
        try (TestCluster cluster = new TestCluster(basePort)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            long before = leader.replicationMessagesSent();
            int lastIndex = -1;
            for (int i = 0; i < 400; i++) {
                lastIndex = leader.appendWrite("linger-" + (i % 50), "v" + i, "linger-op-" + i);
                Thread.sleep(1);
            }
            leader.awaitCommitIndex(lastIndex).get(30, TimeUnit.SECONDS);
            return leader.replicationMessagesSent() - before;
        } finally {
            ServerImpl.applyReplicationSettingsForTest(4000, 4);
        }
    }
}
