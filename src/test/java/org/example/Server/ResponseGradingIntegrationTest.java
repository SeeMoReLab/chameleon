package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.Client.ClientGrader;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.example.Utility.RungScorer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Step 7 end to end against a real cluster: every served reply carries
 * enough (value indices of both views, log and commit indices, delivered
 * level, predicted profit) for the client to grade it on its own, and the
 * client's grade never falls below what the server delivered.
 *
 * Latencies handed to the grader are tiny and thresholds generous, so grading
 * is decided by consistency alone and never by a timing race.
 */
class ResponseGradingIntegrationTest {

    private static final double LATENCY_MS = 1.0;

    private static RungScorer.Rung read(ReadLevel level, double thresholdMs, double profit) {
        return new RungScorer.Rung(level.getNumber(), thresholdMs, profit);
    }

    // App 40: SLA 1 pays for majority levels, SLA 2 is a single eventual-local
    // rung (its floor), write SLA 1 pays 5 at wc:2 and 2 at wc:1.
    private static final List<RungScorer.Rung> READ_SLA_MAJORITY = List.of(
            read(ReadLevel.CAUSAL_MAJORITY, 2000, 4), read(ReadLevel.EVENTUAL_MAJORITY, 1000, 2));
    private static final List<RungScorer.Rung> READ_SLA_FLOOR = List.of(
            read(ReadLevel.EVENTUAL_LOCAL, 1000, 1));
    private static final List<RungScorer.Rung> WRITE_SLA = List.of(
            new RungScorer.Rung(2, 2000, 5), new RungScorer.Rung(1, 1000, 2));

    @BeforeAll
    static void registerSlas() {
        MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001);
        SlaRegistry.registerReadSla(40, 1, READ_SLA_MAJORITY);
        SlaRegistry.registerReadSla(40, 2, READ_SLA_FLOOR);
        SlaRegistry.registerWriteSla(40, 1, WRITE_SLA);
    }

    @Test
    void writesAreGradedOnTheDeliveredConcern() throws Exception {
        try (TestCluster cluster = new TestCluster(19300)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                KvResponse response = session.write("w-key", "w-value", 40, 1);
                assertTrue(response.getOk());
                // Free-and-certain cells make wc:2 the strictly better score
                // (E 5 vs 2), so the write upgrades to majority and the wc:2
                // rung pays.
                assertEquals(2, response.getDeliveredWriteConcern());
                assertEquals(5.0, response.getPredictedProfit(), 1e-9);
                ClientGrader.Verdict verdict = ClientGrader.gradeWrite(WRITE_SLA, response, LATENCY_MS);
                assertEquals(2, verdict.gradedStrength());
                assertEquals(0, verdict.satisfiedRung());
                assertEquals(5.0, verdict.realizedProfit(), 1e-9);
                assertEquals(response.getValueIndex(), verdict.creditedValueIndex());
            }
        }
    }

    @Test
    void committedValueUpgradesAnEventualLocalReadForFree() throws Exception {
        try (TestCluster cluster = new TestCluster(19400)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                KvResponse write = session.write("r-key", "r-value", 40, 1);
                assertTrue(write.getOk());

                // Fresh read cells tie every level at E=1 for the floor SLA
                // and the tie breaks to the weakest: eventual-local executes.
                KvResponse floorRead = session.read("r-key", 40, 2, -1);
                assertTrue(floorRead.getOk());
                assertEquals(write.getValueIndex(), floorRead.getValueIndex());
                assertEquals(ReadLevel.EVENTUAL_LOCAL, floorRead.getDeliveredReadLevel());
                // The value is majority-committed and the session has no
                // frontier, so the client grades the delivered result
                // causal-majority: a free upgrade above the floor.
                ClientGrader.Verdict floorVerdict = ClientGrader.gradeRead(READ_SLA_FLOOR, floorRead, -1, LATENCY_MS);
                assertEquals(ReadLevel.CAUSAL_MAJORITY.getNumber(), floorVerdict.gradedStrength());
                assertEquals(0, floorVerdict.satisfiedRung());
                assertEquals(1.0, floorVerdict.realizedProfit(), 1e-9);
                assertEquals(write.getValueIndex(), floorVerdict.creditedValueIndex());

                // The majority SLA is dominated by its causal-majority rung
                // (E 4 vs 2), which the scorer picks outright.
                KvResponse majorityRead = session.read("r-key", 40, 1, write.getValueIndex());
                assertTrue(majorityRead.getOk());
                assertEquals(ReadLevel.CAUSAL_MAJORITY, majorityRead.getDeliveredReadLevel());
                ClientGrader.Verdict majorityVerdict = ClientGrader.gradeRead(READ_SLA_MAJORITY, majorityRead,
                        write.getValueIndex(), LATENCY_MS);
                assertEquals(0, majorityVerdict.satisfiedRung());
                assertEquals(4.0, majorityVerdict.realizedProfit(), 1e-9);
            }
        }
    }

    @Test
    void clientGradeNeverFallsBelowTheDeliveredLevelOverADeterministicSequence() throws Exception {
        try (TestCluster cluster = new TestCluster(19500)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                double predicted = 0;
                long sessionIndex = -1;

                for (int i = 0; i < 30; i++) {
                    KvResponse write = session.write("k" + (i % 5), "v" + i, 40, 1);
                    assertTrue(write.getOk());
                    predicted += write.getPredictedProfit();
                    ClientGrader.Verdict writeVerdict = ClientGrader.gradeWrite(WRITE_SLA, write, LATENCY_MS);
                    assertTrue(writeVerdict.satisfiedRung() >= 0,
                            "with generous thresholds every served write satisfies a rung");
                    sessionIndex = Math.max(sessionIndex, writeVerdict.creditedValueIndex());

                    boolean majority = i % 2 == 0;
                    List<RungScorer.Rung> sla = majority ? READ_SLA_MAJORITY : READ_SLA_FLOOR;
                    KvResponse read = session.read("k" + (i % 5), 40, majority ? 1 : 2, sessionIndex);
                    assertTrue(read.getOk());
                    predicted += read.getPredictedProfit();
                    ClientGrader.Verdict readVerdict = ClientGrader.gradeRead(sla, read, sessionIndex, LATENCY_MS);
                    assertTrue(readVerdict.satisfiedRung() >= 0,
                            "with generous thresholds every served read satisfies a rung");
                    // The server only labels a level whose condition held when
                    // it served; the reply's indices, stamped no earlier, must
                    // let the client re-establish at least that level.
                    assertTrue(readVerdict.gradedStrength() >= read.getDeliveredReadLevel().getNumber(),
                            "delivered " + read.getDeliveredReadLevel() + " but client graded "
                                    + readVerdict.gradedStrength());
                    sessionIndex = Math.max(sessionIndex, readVerdict.creditedValueIndex());
                }
                assertTrue(predicted > 0, "the predicted-profit stream must be populated");
            }
        }
    }
}
