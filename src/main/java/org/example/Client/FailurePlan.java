package org.example.Client;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.example.Utility.ExperimentConfig;

/**
 * The failure schedule as it unfolds. The config states each step by role and time; this
 * turns it into node ids against the cluster as it is when the step falls due, so a LEADER
 * failure takes whoever leads at that moment (after an earlier leader failure, the election's
 * winner), a FOLLOWER failure the lowest-numbered node that neither leads nor is already
 * failed, and a recovery the node that has been failed longest. The driver owns the clock and
 * the admin RPCs; this class owns the bookkeeping, so it can be tested without a cluster.
 */
public final class FailurePlan {

    public enum Action {
        FAIL,
        RECOVER
    }

    /** One scheduled step; {@code target} is LEADER or FOLLOWER for a failure, null for a recovery. */
    public record Step(int atSeconds, Action action, String target) {
    }

    /** A step resolved to the node it acts on. */
    public record Resolved(Step step, int nodeId) {
    }

    private final List<Step> steps;
    private final int numServers;
    private final ArrayDeque<Integer> failed = new ArrayDeque<>(); // oldest failure first
    private int next = 0;

    public FailurePlan(List<Step> steps, int numServers) {
        this.steps = List.copyOf(steps);
        this.numServers = numServers;
    }

    /** The plan a config describes; empty when the config schedules nothing. */
    public static FailurePlan of(ExperimentConfig config) {
        List<Step> steps = new ArrayList<>();
        if (config.failureSchedule != null) {
            for (ExperimentConfig.FailureEvent e : config.failureSchedule) {
                steps.add(new Step(e.atSeconds, e.action.equals("fail") ? Action.FAIL : Action.RECOVER, e.target));
            }
        }
        return new FailurePlan(steps, config.cluster.numServers);
    }

    /** The step that falls due next, or null when the plan is finished or abandoned. */
    public Step nextStep() {
        return next < steps.size() ? steps.get(next) : null;
    }

    /** Give up the remaining steps after an error; the nodes failed so far stay as they are. */
    public void abandon() {
        next = steps.size();
    }

    /** Nodes currently failed, in id order. */
    public Set<Integer> failedNodes() {
        return Collections.unmodifiableSet(new TreeSet<>(failed));
    }

    /**
     * Resolve the next step against the cluster's current healthy leader ({@code leaderId},
     * -1 when none is known) and advance. Throws IllegalStateException when no node qualifies:
     * a LEADER failure without a healthy leader, a FOLLOWER failure with no healthy follower,
     * or a recovery with nothing failed. The caller decides whether that abandons the plan.
     */
    public Resolved resolve(int leaderId) {
        Step step = nextStep();
        if (step == null) {
            throw new IllegalStateException("the failure schedule has no step left");
        }
        int node;
        switch (step.action()) {
            case FAIL -> {
                if (leaderId < 0 || failed.contains(leaderId)) {
                    throw new IllegalStateException("no healthy leader to resolve the " + step.target()
                            + " failure at " + step.atSeconds() + " s against (leader " + leaderId + ", failed "
                            + failedNodes() + ")");
                }
                if (step.target().equals("LEADER")) {
                    node = leaderId;
                } else {
                    node = -1;
                    for (int candidate = 0; candidate < numServers; candidate++) {
                        if (candidate != leaderId && !failed.contains(candidate)) {
                            node = candidate;
                            break;
                        }
                    }
                    if (node < 0) {
                        throw new IllegalStateException("no healthy follower left to fail at " + step.atSeconds()
                                + " s (leader " + leaderId + ", failed " + failedNodes() + ")");
                    }
                }
                failed.addLast(node);
            }
            case RECOVER -> {
                Integer oldest = failed.pollFirst();
                if (oldest == null) {
                    throw new IllegalStateException("nothing to recover at " + step.atSeconds() + " s");
                }
                node = oldest;
            }
            default -> throw new IllegalStateException("unknown action " + step.action());
        }
        next++;
        return new Resolved(step, node);
    }
}
