package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Consumer;

import org.example.Utility.RungScorer;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

/**
 * A failed node answers every request with a fast failure. The session client must learn
 * that the way it learns a rejection, or the node keeps its pre-failure latency window and
 * admit ratio and draws its share of traffic for the whole outage.
 */
class FailedNodeRoutingTest {

    private static final int NODES = 3;
    private static final int VICTIM = 1;
    private static final List<RungScorer.Rung> READ_SLA = List.of(
            new RungScorer.Rung(ReadLevel.EVENTUAL_LOCAL.getNumber(), 1000, 5));
    private static final List<RungScorer.Rung> WRITE_SLA = List.of(new RungScorer.Rung(1, 1000, 5));

    /** Three nodes at the same 2 ms distance; one of them can be switched to failing. */
    private static final class FakeCluster implements KvTransport {
        final AtomicIntegerArray attempts = new AtomicIntegerArray(NODES);
        volatile int failing = -1;
        private final ScheduledExecutorService replies = Executors.newSingleThreadScheduledExecutor();

        @Override
        public int numServers() {
            return NODES;
        }

        @Override
        public int numSites() {
            return 1;
        }

        @Override
        public RequestHandle execute(int siteId, int nodeId, KvRequest request, long remainingDeadlineNanos,
                Consumer<KvResponse> onResponse, Consumer<RpcFailure> onError) {
            attempts.incrementAndGet(nodeId);
            boolean fail = nodeId == failing;
            replies.schedule(() -> {
                KvResponse.Builder reply = KvResponse.newBuilder().setRequestId(request.getRequestId())
                        .setServiceTimeMs(0.1).setServerReplyEpochMs(System.currentTimeMillis());
                if (fail) {
                    reply.setOk(false);
                } else {
                    reply.setOk(true).setDeliveredReadLevel(ReadLevel.EVENTUAL_LOCAL).setValueIndex(0)
                            .setLocalValueIndex(0).setCommittedValueIndex(0).setLogIndex(1).setCommitIndex(1);
                }
                onResponse.accept(reply.build());
            }, 2, TimeUnit.MILLISECONDS);
            return () -> {
            };
        }

        @Override
        public void close() {
            replies.shutdownNow();
        }

        int attemptsAt(int node) {
            return attempts.get(node);
        }
    }

    private static void awaitQuiescent(KvSessionClient client) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(0, client.pendingCount(), "every read must resolve");
    }

    @Test
    void aFailingNodeLosesItsShareOfReadsWithinAFewFailures() throws Exception {
        try (FakeCluster cluster = new FakeCluster();
                KvSessionClient client = new KvSessionClient(1, 0, "site", cluster, ClientMode.CHAMELEON, 32, 3,
                        5_000, Map.of(1, READ_SLA), Map.of(1, WRITE_SLA), 0.0, true, true, 0.9, 7L)) {
            // Healthy: equal distances, so the three nodes share the reads.
            for (int i = 0; i < 90; i++) {
                client.sendRead("k" + (i % 8), 1);
            }
            awaitQuiescent(client);
            int healthyAtVictim = cluster.attemptsAt(VICTIM);
            assertTrue(healthyAtVictim > 10, "with equal distances the victim gets a share before it fails, got "
                    + healthyAtVictim);

            cluster.failing = VICTIM;
            int before = cluster.attemptsAt(VICTIM);
            for (int i = 0; i < 300; i++) {
                client.sendRead("k" + (i % 8), 1);
            }
            awaitQuiescent(client);
            int duringOutage = cluster.attemptsAt(VICTIM) - before;
            int served = cluster.attemptsAt(0) + cluster.attemptsAt(2);
            assertTrue(duringOutage < 60, "the failing node should stop drawing reads after a few failures, "
                    + "but took " + duringOutage + " attempts of 300 (it took " + healthyAtVictim + " of 90 while healthy)");
            assertTrue(served >= 300, "every read must have ended up on a healthy node, healthy attempts " + served);
        }
    }
}
