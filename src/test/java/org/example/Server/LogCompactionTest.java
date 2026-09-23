package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The log is bounded: committed entries are compacted once they trail the
 * commit index by the retention window, and a follower that fell behind the
 * window is brought forward with the leader's committed state rather than
 * the entries it missed. A real 3-node cluster, retention shrunk so both
 * happen within a few hundred writes.
 */
class LogCompactionTest {

    private static final int RETENTION = 50;
    private int savedRetention;

    @BeforeEach
    void shrinkRetention() {
        savedRetention = ServerImpl.logRetentionEntries();
        ServerImpl.applyLogRetentionForTest(RETENTION);
    }

    @AfterEach
    void restoreRetention() {
        ServerImpl.applyLogRetentionForTest(savedRetention);
    }

    private static void writeMany(TestCluster cluster, ServerImpl leader, int from, int to) throws InterruptedException {
        for (int i = from; i < to; i++) {
            leader = cluster.append(leader, "k" + (i % 37), "v" + i, "op" + i, 10_000);
        }
    }

    private static void awaitFirstIndexAbove(ServerImpl node, int index, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (node.log.firstIndex() <= index) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("node " + node.nodeId() + " never compacted past " + index
                        + " (first retained " + node.log.firstIndex() + ", commit " + node.currentCommitIndex() + ")");
            }
            Thread.sleep(20);
        }
    }

    @Test
    void everyNodeCompactsBehindItsCommitIndexAndKeepsTheState() throws Exception {
        try (TestCluster cluster = new TestCluster(18600)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            int writes = 6 * RETENTION;
            writeMany(cluster, leader, 0, writes);
            cluster.awaitCommitted(writes - 1, 15_000);

            for (ServerImpl node : cluster.nodes) {
                // Compaction runs one window at a time, so the node keeps
                // between one and two windows of committed entries.
                awaitFirstIndexAbove(node, writes - 1 - 2 * RETENTION, 5_000);
                int retainedCommitted = node.currentCommitIndex() - node.log.firstIndex() + 1;
                assertTrue(retainedCommitted >= RETENTION && retainedCommitted < 2 * RETENTION,
                        "node " + node.nodeId() + " retains " + retainedCommitted + " committed entries");
                assertEquals(writes - 1, node.lastLogIndex());
                // The state machine is untouched by compaction: every key holds its last write.
                for (int k = 0; k < 37; k++) {
                    int last = writes - 1 - ((writes - 1 - k) % 37);
                    KvStore.Versioned committed = node.kv.readCommitted("k" + k);
                    assertNotNull(committed, "node " + node.nodeId() + " key k" + k);
                    assertEquals("v" + last, committed.value(), "node " + node.nodeId() + " key k" + k);
                    assertEquals(last, committed.index());
                }
            }
            // Writes keep flowing on a compacted log, and the consistency
            // check at the compaction boundary keeps matching.
            writeMany(cluster, leader, writes, writes + RETENTION);
            cluster.awaitCommitted(writes + RETENTION - 1, 15_000);
            assertEquals(0, sumInstalled(cluster), "no follower fell behind, so no snapshot was needed");
        }
    }

    @Test
    void aFollowerBehindTheWindowIsRestoredFromASnapshotAndCatchesUp() throws Exception {
        try (TestCluster cluster = new TestCluster(18700)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            writeMany(cluster, leader, 0, RETENTION);
            cluster.awaitCommitted(RETENTION - 1, 15_000);

            ServerImpl lagging = null;
            for (ServerImpl node : cluster.nodes) {
                if (node != leader) {
                    lagging = node;
                    break;
                }
            }
            assertNotNull(lagging);
            lagging.setDropAllServerNetworkTraffic(true);

            // With one follower gone the other still forms a majority, so the
            // leader commits and compacts far past the lagging node's log.
            int writes = 8 * RETENTION;
            writeMany(cluster, leader, RETENTION, writes);
            for (ServerImpl node : cluster.nodes) {
                if (node != lagging) {
                    awaitFirstIndexAbove(node, RETENTION, 15_000);
                }
            }
            assertTrue(lagging.lastLogIndex() < leader.log.firstIndex(),
                    "the lagging follower's log must end below the leader's first retained entry");

            lagging.setDropAllServerNetworkTraffic(false);
            cluster.awaitCommitted(writes - 1, 20_000);

            assertEquals(1, lagging.snapshotsInstalled.get(), "the follower installs exactly one snapshot");
            assertTrue(leader.snapshotsSent.get() >= 1);
            assertEquals(writes - 1, lagging.lastLogIndex());
            assertTrue(lagging.log.firstIndex() > RETENTION, "the follower's log starts at the snapshot boundary");
            for (int k = 0; k < 37; k++) {
                KvStore.Versioned expected = leader.kv.readCommitted("k" + k);
                KvStore.Versioned actual = lagging.kv.readCommitted("k" + k);
                assertEquals(expected.value(), actual.value(), "key k" + k);
                assertEquals(expected.index(), actual.index(), "key k" + k);
                assertEquals(expected.value(), lagging.kv.readLocal("k" + k).value(), "local view of k" + k);
            }

            // Ordinary replication resumes on the restored follower.
            writeMany(cluster, leader, writes, writes + RETENTION);
            cluster.awaitCommitted(writes + RETENTION - 1, 15_000);
            assertEquals(1, lagging.snapshotsInstalled.get(), "catching up after the snapshot needs no second one");
            assertEquals("v" + (writes + RETENTION - 1), lagging.kv.readCommitted("k" + ((writes + RETENTION - 1) % 37)).value());
        }
    }

    private static int sumInstalled(TestCluster cluster) {
        int n = 0;
        for (ServerImpl node : cluster.nodes) {
            n += node.snapshotsInstalled.get();
        }
        return n;
    }
}
