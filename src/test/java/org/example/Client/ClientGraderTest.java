package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.example.Utility.RungScorer;
import org.example.raft.KvResponse;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.Test;

/**
 * The client-side SLA check: both views are graded, the best-paying rung
 * wins, and successful responses missing all applicable deadlines are marked.
 */
class ClientGraderTest {

    private static final List<RungScorer.Rung> SLA = List.of(
            new RungScorer.Rung(ReadLevel.CAUSAL_MAJORITY.getNumber(), 200, 6),
            new RungScorer.Rung(ReadLevel.CAUSAL_LOCAL.getNumber(), 100, 3),
            new RungScorer.Rung(ReadLevel.EVENTUAL_LOCAL.getNumber(), 100, 1));

    private static KvResponse.Builder response(ReadLevel delivered) {
        return KvResponse.newBuilder().setOk(true).setDeliveredReadLevel(delivered);
    }

    @Test
    void committedViewWinsWhenItPaysTheTopRung() {
        // Commit index covers the session's latest acknowledged write, and
        // both values are committed: the CM rung pays via the committed view.
        KvResponse r = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(9)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 8, 50);
        assertEquals(0, verdict.satisfiedRung());
        assertEquals(6.0, verdict.realizedProfit(), 1e-9);
        assertFalse(verdict.viaLocalView());
        assertFalse(verdict.deadlineViolation());
    }

    @Test
    void localViewWinsWhenOnlyItSatisfiesTheCausalClaim() {
        // The commit index trails both anchors, so the committed view can
        // only pay the EL rung; the local frontier covers the uncommitted
        // anchor, so causal-local pays 3 via the local view.
        KvResponse r = response(ReadLevel.EVENTUAL_LOCAL)
                .setLogIndex(12).setCommitIndex(5)
                .setLocalValueIndex(11).setCommittedValueIndex(4)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 10, 50);
        assertEquals(1, verdict.satisfiedRung());
        assertEquals(3.0, verdict.realizedProfit(), 1e-9);
        assertTrue(verdict.viaLocalView());
        assertFalse(verdict.deadlineViolation());
    }

    @Test
    void serverCausalLabelDoesNotBypassClientFrontierVerification() {
        KvResponse r = response(ReadLevel.CAUSAL_MAJORITY)
                .setLogIndex(7).setCommitIndex(7)
                .setLocalValueIndex(6).setCommittedValueIndex(6)
                .build();

        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 20, 50);

        assertEquals(2, verdict.satisfiedRung(), "the server cannot award itself causal-majority credit");
        assertEquals(1.0, verdict.realizedProfit(), 1e-9);
    }

    @Test
    void causalServerLabelProducesTheSameGradeAsPileusBaseline() {
        KvResponse pileus = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(7).setCommitIndex(10)
                .setLocalValueIndex(6).setCommittedValueIndex(6)
                .build();
        KvResponse chameleon = pileus.toBuilder()
                .setDeliveredReadLevel(ReadLevel.CAUSAL_MAJORITY)
                .build();

        ClientGrader.Verdict pileusVerdict = ClientGrader.gradeRead(SLA, pileus, 20, 50);
        ClientGrader.Verdict chameleonVerdict = ClientGrader.gradeRead(SLA, chameleon, 20, 50);

        assertEquals(pileusVerdict, chameleonVerdict,
                "a causal label must not change the client-authoritative grade");
        assertEquals(2, chameleonVerdict.satisfiedRung());
    }

    @Test
    void linearizableLabelCannotBypassTheClientCausalFrontier() {
        KvResponse r = response(ReadLevel.LINEARIZABLE)
                .setLogIndex(20).setCommitIndex(7)
                .setLocalValueIndex(6).setCommittedValueIndex(6)
                .build();

        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, 20, 50);

        assertEquals(1, verdict.satisfiedRung());
        assertEquals(3.0, verdict.realizedProfit(), 1e-9);
        assertTrue(verdict.viaLocalView());
    }

    @Test
    void latencyBlowingEveryThresholdRealizesNothing() {
        KvResponse r = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(9)
                .build();
        ClientGrader.Verdict verdict = ClientGrader.gradeRead(SLA, r, -1, 500);
        assertEquals(-1, verdict.satisfiedRung());
        assertEquals(0.0, verdict.realizedProfit(), 1e-9);
        assertTrue(verdict.deadlineViolation());
    }

    @Test
    void creditedValueIndexFollowsTheWinningView() {
        // Both views pay causal-majority: the tie goes to the committed view,
        // so the frontier advances to the committed value's write.
        KvResponse committedWins = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(7)
                .build();
        assertEquals(7, ClientGrader.gradeRead(SLA, committedWins, 8, 50).creditedValueIndex());
        // Only the local view satisfies the causal claim: its value is the
        // one surfaced, so its write is the one the session now depends on.
        KvResponse localWins = response(ReadLevel.EVENTUAL_LOCAL)
                .setLogIndex(12).setCommitIndex(5)
                .setLocalValueIndex(11).setCommittedValueIndex(4)
                .build();
        assertEquals(11, ClientGrader.gradeRead(SLA, localWins, 10, 50).creditedValueIndex());
        // An absent key is not an observation.
        KvResponse absent = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(-1).setCommittedValueIndex(-1)
                .build();
        assertEquals(-1, ClientGrader.gradeRead(SLA, absent, -1, 50).creditedValueIndex());
    }

    @Test
    void gradingUsesOnlyTheIndexItIsHanded() {
        // The grader is a pure function of the index the request carried: the
        // same response grades differently under a frontier the view covers
        // (8) and one it does not (11), and nothing else can influence it.
        KvResponse r = response(ReadLevel.EVENTUAL_MAJORITY)
                .setLogIndex(10).setCommitIndex(10)
                .setLocalValueIndex(9).setCommittedValueIndex(9)
                .build();
        assertEquals(0, ClientGrader.gradeRead(SLA, r, 8, 50).satisfiedRung());
        assertEquals(2, ClientGrader.gradeRead(SLA, r, 11, 50).satisfiedRung());
    }

    @Test
    void writesGradeOnTheDeliveredConcern() {
        List<RungScorer.Rung> writeSla = List.of(
                new RungScorer.Rung(2, 300, 8),
                new RungScorer.Rung(1, 150, 3));
        KvResponse ack = KvResponse.newBuilder().setOk(true).setDeliveredWriteConcern(2).build();
        ClientGrader.Verdict verdict = ClientGrader.gradeWrite(writeSla, ack, 100);
        assertEquals(0, verdict.satisfiedRung());
        assertEquals(8.0, verdict.realizedProfit(), 1e-9);
        assertFalse(verdict.deadlineViolation());
        ClientGrader.Verdict late = ClientGrader.gradeWrite(writeSla, ack, 400);
        assertEquals(-1, late.satisfiedRung());
        assertTrue(late.deadlineViolation());
    }
}
