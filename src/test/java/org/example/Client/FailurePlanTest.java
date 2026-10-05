package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.example.Client.FailurePlan.Action;
import org.example.Client.FailurePlan.Step;
import org.example.Utility.ExperimentConfig;
import org.junit.jupiter.api.Test;

/** The schedule's steps resolve against the cluster of the moment, and recoveries are first-failed, first-recovered. */
class FailurePlanTest {

    private static Step fail(int at, String target) {
        return new Step(at, Action.FAIL, target);
    }

    private static Step recover(int at) {
        return new Step(at, Action.RECOVER, null);
    }

    @Test
    void leaderFailuresTakeWhoeverLeadsAtTheMoment() {
        FailurePlan plan = new FailurePlan(List.of(fail(150, "LEADER"), fail(270, "LEADER"), recover(390), recover(510)), 5);
        assertEquals(150, plan.nextStep().atSeconds());
        assertEquals(0, plan.resolve(0).nodeId(), "the first failure takes the current leader");
        assertEquals(Set.of(0), plan.failedNodes());
        assertEquals(3, plan.resolve(3).nodeId(), "the second takes the election's winner");
        assertEquals(Set.of(0, 3), plan.failedNodes());
        assertEquals(0, plan.resolve(-1).nodeId(), "the first recovery returns the node failed longest");
        assertEquals(3, plan.resolve(-1).nodeId());
        assertTrue(plan.failedNodes().isEmpty());
        assertNull(plan.nextStep(), "the plan is finished");
    }

    @Test
    void followerFailuresSkipTheLeaderAndNodesAlreadyFailed() {
        FailurePlan plan = new FailurePlan(List.of(fail(150, "FOLLOWER"), fail(270, "FOLLOWER"), recover(390)), 5);
        assertEquals(1, plan.resolve(0).nodeId(), "node 0 leads, so the lowest healthy follower is 1");
        assertEquals(2, plan.resolve(0).nodeId(), "node 1 is already failed");
        assertEquals(1, plan.resolve(0).nodeId(), "recovery brings back node 1 first");
    }

    @Test
    void aFailureWithoutAHealthyLeaderIsAnError() {
        FailurePlan plan = new FailurePlan(List.of(fail(150, "LEADER"), fail(270, "FOLLOWER")), 5);
        plan.resolve(2);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> plan.resolve(-1));
        assertTrue(e.getMessage().contains("no healthy leader"), e.getMessage());
        assertThrows(IllegalStateException.class, () -> plan.resolve(2),
                "a leader id that is itself failed is stale and cannot be resolved against");
        assertEquals(fail(270, "FOLLOWER"), plan.nextStep(), "a failed resolution does not advance the plan");
        plan.abandon();
        assertNull(plan.nextStep());
        assertEquals(Set.of(2), plan.failedNodes(), "abandoning leaves the failed nodes as they are");
    }

    @Test
    void recoveringWithNothingFailedIsAnError() {
        FailurePlan plan = new FailurePlan(List.of(recover(10)), 3);
        assertThrows(IllegalStateException.class, () -> plan.resolve(0));
    }

    @Test
    void theRepoFailureConfigsDescribeTheExperiment() {
        for (String name : List.of("config_failure_leader_global.yaml", "config_failure_follower_global.yaml")) {
            ExperimentConfig config = ExperimentConfig.load(Path.of("..", name));
            FailurePlan plan = FailurePlan.of(config);
            assertEquals(150, plan.nextStep().atSeconds());
            assertEquals(Action.FAIL, plan.nextStep().action());
            String role = name.contains("leader") ? "LEADER" : "FOLLOWER";
            assertEquals(role, plan.nextStep().target());
            plan.resolve(0);
            plan.resolve(role.equals("LEADER") ? 1 : 0);
            assertEquals(Action.RECOVER, plan.nextStep().action());
            assertEquals(390, plan.nextStep().atSeconds());
            plan.resolve(-1);
            assertEquals(510, plan.nextStep().atSeconds());
            plan.resolve(-1);
            assertNull(plan.nextStep());
        }
        assertNull(FailurePlan.of(ExperimentConfig.load(Path.of("..", "config_global.yaml"))).nextStep(),
                "the day-sequence configs schedule no failures");
    }
}
