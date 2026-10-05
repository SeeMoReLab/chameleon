package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.raft.KvResponse;
import org.example.Utility.RungScorer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A write that takes a cold-start rider and is then rejected must give the
 * rider back to the cell it came from. The leader once released it by write
 * concern instead of by the write cell's index, so every budget rejection
 * leaked one rider; after 64 the cell could never be probed again, and an SLA
 * whose rungs all need a majority was rejected for the rest of the run.
 */
class RiderReleaseTest {

    @BeforeAll
    static void economicsDefaults() {
        MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001, 1.0e6);
    }

    @Test
    void writesRejectedByTheReplicationBudgetReturnTheirRiders() throws Exception {
        int majority = TestCluster.NUM_NODES / 2 + 1;
        // One rung, majority only: on a fresh leader its cell is empty, so every
        // admitted attempt rides the cold-start cap.
        SlaRegistry.registerWriteSla(40, 1, List.of(new RungScorer.Rung(majority, 1000, 5)));
        double budget = KvClientService.replicationBudgetForTest();
        // Far below one entry: the bucket starts and stays empty for the test.
        KvClientService.setReplicationBudgetForTest(0.001);
        try (TestCluster cluster = new TestCluster(19950)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            MeasurementPlane plane = cluster.planes.get(leader.nodeId());
            int cell = plane.writeLevelIndex(majority);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                for (int i = 0; i < 100; i++) {
                    KvResponse reply = session.write("k" + i, "v", 40, 1);
                    assertTrue(reply.getRejected(), "write " + i + " must be rejected by the empty budget");
                }
            }
            assertEquals(0, plane.uncalibratedRidersForTest(cell, 0),
                    "every rejected write must return its rider to the majority write cell");
            for (int level = 0; level < plane.riderLevelsForTest(); level++) {
                assertEquals(0, plane.uncalibratedRidersForTest(level, 0),
                        "no other cell's counter may be touched, level " + level);
            }
            assertTrue(plane.histograms().snapshot(cell, 0).totalCount <= 0,
                    "nothing was admitted, so the cell is still uncalibrated");
        } finally {
            KvClientService.setReplicationBudgetForTest(budget);
        }
    }
}
