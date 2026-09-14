package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.example.Utility.RungScorer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The measurement plane against a live cluster: cells populate under real
 * traffic, the abandoned-wait rule files at the wait bound under the chosen
 * level, and the occupancy integral matches the sum of completed service
 * times (they measure the same quantity, so with nothing in flight they must
 * agree tightly).
 */
class MeasurementPlaneIntegrationTest {

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
    void cellsPopulateAndOccupancyMatchesCompletedServiceTimes() throws Exception {
        SlaRegistry.registerReadSla(20, 1, List.of(read(ReadLevel.EVENTUAL_LOCAL, 1000, 2)));
        SlaRegistry.registerWriteSla(20, 1, List.of(write(1, 1000, 2)));
        SlaRegistry.registerReadSla(21, 1, List.of(read(ReadLevel.LINEARIZABLE, 1000, 5)));
        SlaRegistry.registerWriteSla(21, 1, List.of(write(2, 1000, 5)));

        try (TestCluster cluster = new TestCluster(18900)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            MeasurementPlane plane = cluster.planes.get(leader.nodeId());

            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                for (int i = 0; i < 300; i++) {
                    int app = (i % 2 == 0) ? 20 : 21;
                    session.write("k" + (i % 10), "v" + i, app, 1);
                    session.read("k" + (i % 10), app, 1, -1);
                }

                // Let the refresh tick publish and the last replies settle.
                Thread.sleep(400);

                ServiceTimeHistograms h = plane.histograms();
                assertTrue(h.snapshot(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0).totalCount > 0,
                        "eventual-local cell must populate");
                assertTrue(h.snapshot(plane.readLevelIndex(ReadLevel.LINEARIZABLE), 0).totalCount > 0,
                        "linearizable cell must populate");
                assertTrue(h.snapshot(plane.writeLevelIndex(1), 0).totalCount > 0, "wc:1 cell must populate");
                assertTrue(h.snapshot(plane.writeLevelIndex(2), 0).totalCount > 0, "wc:2 cell must populate");

                // Linearizable pays a confirmation round; its mean service
                // time must exceed the immediate eventual-local reads.
                double linMean = h.snapshot(plane.readLevelIndex(ReadLevel.LINEARIZABLE), 0).meanMs;
                double eventualMean = h.snapshot(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0).meanMs;
                assertTrue(linMean > eventualMean,
                        "LIN mean " + linMean + " must exceed eventual mean " + eventualMean);

                // Nothing in flight now, so the occupancy integral and the sum
                // of completed service times measure exactly the same thing.
                assertEquals(0, plane.inFlight(), "all requests replied");
                double slotMs = plane.cumulativeSlotNanos() / 1_000_000.0;
                double completedMs = plane.cumulativeCompletedServiceMs();
                assertTrue(completedMs > 0);
                // The open tail (since the last interval close) is not yet in
                // cumulativeSlotNanos, so allow a proportional margin.
                assertEquals(1.0, slotMs / completedMs, 0.15,
                        "occupancy integral " + slotMs + " ms vs completed service " + completedMs + " ms");
            }
        }
    }

    @Test
    void abandonedWaitFilesAtTheBoundUnderTheChosenLevel() throws Exception {
        // Single causal-local rung, 300 ms threshold: d_max = 300 is the wait
        // bound and the filing value for an abandoned wait.
        SlaRegistry.registerReadSla(22, 1, List.of(read(ReadLevel.CAUSAL_LOCAL, 300, 3)));

        try (TestCluster cluster = new TestCluster(19000)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            ServerImpl follower = cluster.nodes.stream().filter(n -> n != leader).findFirst().orElseThrow();
            MeasurementPlane plane = cluster.planes.get(follower.nodeId());

            try (TestSession session = new TestSession(cluster.portOf(follower.nodeId()))) {
                // Unreachable anchor in the far-behind gap bucket: the wait
                // expires, the fallback serves, and the sample lands in the
                // causal-local far-gap cell at the bound.
                int unreachable = follower.lastLogIndex() + 100_000;
                KvResponse readResponse = session.read("any", 22, 1, unreachable);
                assertTrue(readResponse.getTimedOutAndFellBack());
                assertEquals(ReadLevel.EVENTUAL_LOCAL, readResponse.getDeliveredReadLevel());

                Thread.sleep(300); // let the tick publish

                int causalLocal = plane.readLevelIndex(ReadLevel.CAUSAL_LOCAL);
                ServiceTimeHistograms.Snapshot farGap = plane.histograms().snapshot(causalLocal, 3);
                assertTrue(farGap.totalCount > 0, "abandoned wait must file under the chosen level's cell");
                assertEquals(300.0, farGap.meanMs, 1.0,
                        "abandoned wait files at the wait bound (d_max), got " + farGap.meanMs);

                // Nothing under the fallback level: its cell must not have
                // absorbed the abandoned wait.
                ServiceTimeHistograms.Snapshot fallbackCell = plane.histograms()
                        .snapshot(plane.readLevelIndex(ReadLevel.EVENTUAL_LOCAL), 0);
                assertEquals(0.0, fallbackCell.totalCount, 1e-9,
                        "the fallback level's cell must stay empty for an abandoned wait");
            }
        }
    }
}
