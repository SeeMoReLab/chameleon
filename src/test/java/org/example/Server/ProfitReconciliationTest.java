package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.example.Utility.Grading;
import org.example.Utility.RungScorer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Step 7 end to end: the profit accounting reconciles. Every served response
 * carries (satisfiedRung, predictedProfit, realizedProfit, graded strength);
 * regrading each per-request record independently from the response's own
 * fields (value index, commit/log indices, delivered level, service time,
 * anchors sent) must reproduce the server's numbers exactly, so the sum of
 * realized profit matches the grading of the per-request records.
 *
 * Thresholds are far above real service times so grading is decided by
 * consistency alone and never by a timing race.
 */
class ProfitReconciliationTest {

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

    /** Regrade one read record from nothing but the response and the request's anchors. */
    private static Grading.Realized regradeRead(List<RungScorer.Rung> sla, KvResponse response,
            int sessionIndex) {
        ReadLevel delivered = response.getDeliveredReadLevel();
        boolean localView = delivered == ReadLevel.EVENTUAL_LOCAL || delivered == ReadLevel.CAUSAL_LOCAL;
        int graded = Grading.gradeRead(delivered, response.getValueIndex(), response.getCommitIndex(),
                localView ? response.getLogIndex() : response.getCommitIndex(),
                sessionIndex);
        assertEquals(graded, response.getGradedReadStrength(), "server and record grading must agree");
        return Grading.realize(sla, graded, response.getServiceTimeMs());
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
                assertEquals(0, response.getSatisfiedRung());
                assertEquals(5.0, response.getRealizedProfit(), 1e-9);
                assertEquals(5.0, response.getPredictedProfit(), 1e-9);
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
                assertEquals("r-value", floorRead.getValue());
                assertEquals(ReadLevel.EVENTUAL_LOCAL, floorRead.getDeliveredReadLevel());
                // The value is majority-committed and the session has no
                // anchors, so the delivered result grades causal-majority:
                // a free upgrade above the floor.
                assertEquals(ReadLevel.CAUSAL_MAJORITY.getNumber(), floorRead.getGradedReadStrength());
                assertEquals(0, floorRead.getSatisfiedRung());
                assertEquals(1.0, floorRead.getRealizedProfit(), 1e-9);

                // The majority SLA is dominated by its causal-majority rung
                // (E 4 vs 2), which the scorer picks outright.
                KvResponse majorityRead = session.read("r-key", 40, 1, write.getValueIndex());
                assertTrue(majorityRead.getOk());
                assertEquals(ReadLevel.CAUSAL_MAJORITY, majorityRead.getDeliveredReadLevel());
                assertEquals(0, majorityRead.getSatisfiedRung());
                assertEquals(4.0, majorityRead.getRealizedProfit(), 1e-9);
            }
        }
    }

    @Test
    void profitAccountingReconcilesOverADeterministicSequence() throws Exception {
        try (TestCluster cluster = new TestCluster(19500)) {
            ServerImpl leader = cluster.awaitLeader(15_000);
            try (TestSession session = new TestSession(cluster.portOf(leader.nodeId()))) {
                double reportedRealized = 0;
                double regradedRealized = 0;
                double reportedPredicted = 0;
                int sessionIndex = -1;
                List<KvResponse> served = new ArrayList<>();

                for (int i = 0; i < 30; i++) {
                    KvResponse write = session.write("k" + (i % 5), "v" + i, 40, 1);
                    assertTrue(write.getOk());
                    sessionIndex = Math.max(sessionIndex, write.getValueIndex());
                    reportedRealized += write.getRealizedProfit();
                    reportedPredicted += write.getPredictedProfit();
                    regradedRealized += Grading.realize(WRITE_SLA, write.getDeliveredWriteConcern(),
                            write.getServiceTimeMs()).profit();
                    served.add(write);

                    List<RungScorer.Rung> sla = (i % 2 == 0) ? READ_SLA_MAJORITY : READ_SLA_FLOOR;
                    KvResponse readResponse = session.read("k" + (i % 5), 40, (i % 2 == 0) ? 1 : 2, sessionIndex);
                    assertTrue(readResponse.getOk());
                    reportedRealized += readResponse.getRealizedProfit();
                    reportedPredicted += readResponse.getPredictedProfit();
                    regradedRealized += regradeRead(sla, readResponse, sessionIndex).profit();
                    served.add(readResponse);
                }

                assertEquals(regradedRealized, reportedRealized, 1e-9,
                        "sum of realized profit must match regrading the per-request records");
                assertTrue(reportedPredicted > 0, "the predicted-profit stream must be populated");
                for (KvResponse response : served) {
                    assertTrue(response.getSatisfiedRung() >= 0,
                            "with generous thresholds every served request satisfies a rung");
                }
            }
        }
    }
}
