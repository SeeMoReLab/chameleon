package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.raft.ReadLevel;
import org.example.Utility.RungScorer;
import org.junit.jupiter.api.Test;

/**
 * Admission economics end to end: under light load lambda stays at the floor
 * and nothing is rejected; under artificial pressure (tiny S_max) lambda
 * rises multiplicatively and requests are shed in value order - the
 * low-profit SLA is rejected while the high-profit SLA is still being
 * served.
 */
class EconomicsIntegrationTest {

    private static RungScorer.Rung read(ReadLevel level, double thresholdMs, double profit) {
        return new RungScorer.Rung(level.getNumber(), thresholdMs, profit);
    }

    @Test
    void lightLoadRejectsNothingAndLambdaStaysAtTheFloor() throws Exception {
        MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001);
        SlaRegistry.registerReadSla(30, 1, List.of(read(ReadLevel.EVENTUAL_LOCAL, 1000, 2)));
        SlaRegistry.registerWriteSla(30, 1, List.of(new RungScorer.Rung(1, 1000, 2)));

        try (TestCluster cluster = new TestCluster(19100)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            MeasurementPlane plane = cluster.planes.get(leader.nodeId());

            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                int rejected = 0;
                for (int i = 0; i < 200; i++) {
                    if (session.write("k" + (i % 10), "v" + i, 30, 1).getRejected()) {
                        rejected++;
                    }
                    if (session.read("k" + (i % 10), 30, 1, -1).getRejected()) {
                        rejected++;
                    }
                }
                assertEquals(0, rejected, "light load must not shed anything");
                assertTrue(plane.lambda() <= 0.001,
                        "lambda must sit at the floor under light load, got " + plane.lambda());
            }
        }
    }

    @Test
    void shedsInValueOrderAsThePriceRises() throws Exception {
        // The price is injected directly at three operating points (floor,
        // mid, extreme) rather than manufactured through utilization: the
        // controller's closed loop is unit-tested and visible in the
        // light-load test; what this test pins is that scoring and admission
        // shed strictly in value order as lambda rises. A near-zero eta makes
        // the background controller hold the forced price.
        MeasurementPlane.applyEconomics(1000, 50, 0.85, 1e-9, 0.0001);
        try {
            // Same rung shape, five orders of magnitude apart in profit.
            SlaRegistry.registerReadSla(31, 1, List.of(read(ReadLevel.EVENTUAL_LOCAL, 1000, 0.001)));
            SlaRegistry.registerReadSla(32, 1, List.of(read(ReadLevel.EVENTUAL_LOCAL, 1000, 100)));

            try (TestCluster cluster = new TestCluster(19200)) {
                ServerImpl leader = cluster.awaitLeader(15_000);
                MeasurementPlane plane = cluster.planes.get(leader.nodeId());

                try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
    // Warm every read cell: with cells empty the cold-start
                    // rule makes any level free and certain, so a forced high
                    // price would just push traffic onto uncalibrated levels.
                    // Each histogram tick publishes the newly calibrated level
                    // and the tie-break moves to the next empty one, so the
                    // warm-up must span several ticks to cascade through the
                    // ladder.
                    int lowAcceptedAtFloor = 0;
                    boolean allWarm = false;
                    long warmDeadline = System.currentTimeMillis() + 10_000;
                    while (!allWarm && System.currentTimeMillis() < warmDeadline) {
                        if (!session.read("k", 31, 1, -1).getRejected()) {
                            lowAcceptedAtFloor++;
                        }
                        session.read("k", 32, 1, -1);
                        Thread.sleep(5);
                        allWarm = true;
                        for (int level = 0; level < 5; level++) {
                            if (plane.histograms().snapshot(level, 0).totalCount <= 0) {
                                allWarm = false;
                                break;
                            }
                        }
                    }
                    assertTrue(lowAcceptedAtFloor > 0, "at the price floor the low-profit SLA must be served");
                    assertTrue(allWarm, "warm-up must calibrate every read level's cell");

                    double minOmega = Double.MAX_VALUE;
                    for (int level = 0; level < 5; level++) {
                        minOmega = Math.min(minOmega,
                                Math.max(plane.histograms().snapshot(level, 0).meanMs, 0.001));
                    }

                    // Mid price: expensive enough that 0.001 profit cannot pay
                    // for any level's slot time, cheap enough that 100 can.
                    plane.forceLambdaForTest(10.0 / minOmega);
                    int lowRejected = 0;
                    int highAccepted = 0;
                    for (int i = 0; i < 50; i++) {
                        if (session.read("k", 31, 1, -1).getRejected()) {
                            lowRejected++;
                        }
                        if (!session.read("k", 32, 1, -1).getRejected()) {
                            highAccepted++;
                        }
                    }
                    assertEquals(50, lowRejected, "at the mid price every low-profit request is shed");
                    assertEquals(50, highAccepted, "at the mid price the high-profit SLA is still served");

                    // Extreme price: nothing is worth what it consumes.
                    plane.forceLambdaForTest(100_000.0 / minOmega);
                    int highRejected = 0;
                    for (int i = 0; i < 50; i++) {
                        if (session.read("k", 32, 1, -1).getRejected()) {
                            highRejected++;
                        }
                    }
                    assertEquals(50, highRejected, "at the extreme price even the high-profit SLA is shed");
                }
            }
        } finally {
            MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001);
        }
    }
}
