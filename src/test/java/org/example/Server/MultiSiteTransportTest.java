package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.example.Client.KvFramedTransport;
import org.example.Utility.RungScorer;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The framed transport's per-site connection pools: requests sent through
 * different sites of one transport reach the same servers and resolve
 * normally (source binding is a CloudLab concern; sites here are unbound),
 * and the single-site convenience entry point refuses a multi-site transport
 * instead of silently picking a site.
 */
class MultiSiteTransportTest {

    @BeforeAll
    static void economicsDefaults() {
        MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001);
    }

    private static KvResponse call(KvFramedTransport transport, int siteId, int nodeId,
            org.example.raft.KvRequest.Builder request) throws Exception {
        CompletableFuture<KvResponse> future = new CompletableFuture<>();
        transport.execute(siteId, nodeId, request.build(), TimeUnit.SECONDS.toNanos(10),
                future::complete,
                failure -> future.completeExceptionally(failure.status().asRuntimeException()));
        return future.get(10, TimeUnit.SECONDS);
    }

    @Test
    void sitesShareServersAndResolveIndependently() throws Exception {
        SlaRegistry.registerWriteSla(60, 1,
                List.of(new RungScorer.Rung(1, 1000, 5)));
        SlaRegistry.registerReadSla(60, 1,
                List.of(new RungScorer.Rung(ReadLevel.EVENTUAL_LOCAL.getNumber(), 1000, 5)));

        try (TestCluster cluster = new TestCluster(18500)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            List<String> hosts = Collections.nCopies(TestCluster.NUM_NODES, "localhost");
            List<String> unboundSites = java.util.Arrays.asList(null, null);
            try (KvFramedTransport transport = new KvFramedTransport(
                    hosts, cluster.clientBasePort, 1, unboundSites)) {
                assertEquals(2, transport.numSites());
                assertEquals(TestCluster.NUM_NODES, transport.numServers());

                long id = 1;
                KvResponse write = call(transport, 0, leader.nodeId(),
                        TestSession.writeRequest("msite", "v0", 60, 1).setRequestId(id++));
                assertTrue(write.getOk(), "write via site 0 must be served");

                KvResponse read = call(transport, 1, leader.nodeId(),
                        TestSession.readRequest("msite", 60, 1, -1).setRequestId(id++));
                assertTrue(read.getOk(), "read via site 1 must be served");
                assertEquals(write.getValueIndex(), read.getValueIndex());

                // The site-less entry point is only for single-site transports.
                assertThrows(IllegalStateException.class, () -> transport.execute(
                        leader.nodeId(),
                        TestSession.readRequest("msite", 60, 1, -1).setRequestId(99).build(),
                        TimeUnit.SECONDS.toNanos(1), r -> { }, f -> { }));
            }
        }
    }
}
