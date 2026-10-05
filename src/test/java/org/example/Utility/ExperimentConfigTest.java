package org.example.Utility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * The strict loader over YAML: the repo's local config must load and
 * validate, and the unknown-key typo protection must work through the YAML
 * path exactly as it does for JSON.
 */
class ExperimentConfigTest {

    // Surefire's working directory is the build directory, a direct child of
    // the repo root, so the repo config is one level up.
    private static final Path REPO_CONFIG = Path.of("..", "config_local.yaml");
    private static final Path MONGODB_CONFIG = Path.of("..", "config_global_mongodb.yaml");

    @Test
    void repoConfigLoadsAndValidates() {
        assertTrue(Files.exists(REPO_CONFIG), "repo config_local.yaml must exist at " + REPO_CONFIG.toAbsolutePath());
        ExperimentConfig config = ExperimentConfig.load(REPO_CONFIG);
        assertEquals(config.cluster.serverHosts.size(), (int) config.cluster.numServers);
        assertTrue(config.slas.size() >= 1);
        assertTrue(config.server.maxEntriesPerReplicationBatch > 0);
        assertTrue(config.server.maxInflightReplicationBatchesPerFollower > 0);
        assertTrue(config.server.replicationFlowControlWindowBytes >= 65536);
        // The mode helpers agree with the mode string.
        assertEquals(config.mode.startsWith("chameleon"), config.chameleonDecision());
    }

    @Test
    void unknownKeysAreRejectedThroughYaml() throws IOException {
        String yaml = Files.readString(REPO_CONFIG) + "\nnotARealKey: 42\n";
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("notARealKey"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void mixEntriesMustReferenceRegisteredSlas() throws IOException {
        // Point one mix entry at an application that registers no SLAs. The
        // pattern matches only uncommented entries (a comment line cannot
        // start with "- {"), so the test holds regardless of which entries
        // the repo config currently has commented out.
        String yaml = Files.readString(REPO_CONFIG)
                .replaceFirst("(?m)^(\\s*)- \\{ applicationId: \\d+", "$1- { applicationId: 9");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("unregistered SLA 9/"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void phasesMustReferenceDefinedMixes() throws IOException {
        // Insert the extra phase immediately after "  phases:" rather than at the
        // end of the file. Appending relied on workload.phases being the last
        // section, which it no longer is - and that same assumption, in a config
        // generator, once silently dropped a trailing transport declaration.
        String repo = Files.readString(REPO_CONFIG);
        int at = repo.indexOf("\n  phases:\n");
        assertTrue(at >= 0, "repo config has no '  phases:' block");
        at += "\n  phases:\n".length();
        String yaml = repo.substring(0, at)
                + "    - name: Bad\n"
                + "      durationSeconds: 10\n"
                + "      totalTPS: 100\n"
                + "      mix: doesNotExist\n"
                + repo.substring(at);
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("doesNotExist"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void invalidValuesFailFast() throws IOException {
        String yaml = Files.readString(REPO_CONFIG).replaceFirst("mode: \\w+", "mode: turbo");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("mode"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void clientSiteLatencyRowsMustMatchNumServers() throws IOException {
        // Drop the first entry of the first site's latency row (the only
        // "latencyMs: [" occurrences in the config are the site rows).
        String original = Files.readString(REPO_CONFIG);
        String yaml = original.replaceFirst("latencyMs: \\[[^,]+, ", "latencyMs: [");
        assertTrue(!yaml.equals(original), "test setup: the replacement must have matched a site row");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("latencyMs"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void sessionsMustSpreadEvenlyAcrossClientSites() throws IOException {
        // The repo config's session count divides its site count; one more
        // session breaks the even spread and must be rejected explicitly.
        ExperimentConfig valid = ExperimentConfig.load(REPO_CONFIG);
        int uneven = valid.workload.sessionsPerApplication + 1;
        String yaml = Files.readString(REPO_CONFIG)
                .replaceFirst("sessionsPerApplication: \\d+", "sessionsPerApplication: " + uneven)
                .replaceFirst("injectorThreads: \\d+", "injectorThreads: 1");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("divisible"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void lambdaMinZeroIsAcceptedButNegativeIsNot() throws IOException {
        // lambdaMin: 0 is the priceless-chameleon ablation (the price stays
        // pinned at zero); a negative floor stays rejected.
        String base = Files.readString(REPO_CONFIG);
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, base.replaceFirst("lambdaMin: [0-9.]+", "lambdaMin: 0"));
            assertEquals(0.0, ExperimentConfig.load(tmp).chameleon.lambdaMin, 0.0);
            Files.writeString(tmp, base.replaceFirst("lambdaMin: [0-9.]+", "lambdaMin: -0.1"));
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("lambdaMin"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void replicationFlowControlWindowBelowHttp2DefaultIsRejected() throws IOException {
        // A window below the HTTP/2 default 64 KiB could not even hold one
        // replication batch and must fail fast rather than throttle silently.
        String yaml = Files.readString(REPO_CONFIG).replaceFirst(
                "replicationFlowControlWindowBytes: \\d+", "replicationFlowControlWindowBytes: 1024");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("replicationFlowControlWindowBytes"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void clientSiteBindHostsFollowTheFixedConvention() {
        assertEquals("127.0.2.1", ExperimentConfig.clientSiteBindHost(0));
        assertEquals("127.0.2.6", ExperimentConfig.clientSiteBindHost(5));
        assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.clientSiteBindHost(-1));
        assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.clientSiteBindHost(254));
    }

    /**
     * A distributed run (run_all_cloudlab.sh --hosts) writes
     * geo.clientSiteBindHosts into the config, because the sites then live on
     * a machine the servers reach over the network and loopback addresses
     * would not be routable.
     */
    @Test
    void clientSiteBindHostsOverrideTheConventionWhenTheConfigNamesThem() throws IOException {
        int sites = ExperimentConfig.load(REPO_CONFIG).geo.clientSites.size();
        StringBuilder list = new StringBuilder();
        for (int s = 0; s < sites; s++) {
            list.append(s > 0 ? ", " : "").append("10.99.0.").append(s + 1);
        }
        ExperimentConfig config = loadWithBindHosts(list.toString());
        assertEquals("10.99.0.1", config.siteBindHost(0));
        assertEquals("10.99.0." + sites, config.siteBindHost(sites - 1));
        // Absent, the single-host loopback convention still applies.
        assertEquals("127.0.2.1", ExperimentConfig.load(REPO_CONFIG).siteBindHost(0));
    }

    @Test
    void clientSiteBindHostsAreRejectedWhenTheyCannotIdentifySites() {
        // One per site, or the delay rules and the sites disagree about which is which.
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> loadWithBindHosts("10.99.0.1, 10.99.0.2")).getMessage()
                .contains("one entry per client site"));
        // Two sites sharing a source address are indistinguishable to netem.
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> loadWithBindHosts("10.99.0.1, 10.99.0.1, 10.99.0.3, 10.99.0.4, 10.99.0.5")).getMessage()
                .contains("duplicated"));
        // A hostname would resolve differently on each machine.
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> loadWithBindHosts("siteA, siteB, siteC, siteD, siteE")).getMessage()
                .contains("literal IPv4 address"));
    }

    /** The repo config with a geo.clientSiteBindHosts line spliced in, the way the runner writes it. */
    private static ExperimentConfig loadWithBindHosts(String list) throws IOException {
        String yaml = Files.readString(REPO_CONFIG)
                .replaceFirst("(?m)^geo:$", "geo:\n  clientSiteBindHosts: [" + list + "]");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            return ExperimentConfig.load(tmp);
        } finally {
            Files.delete(tmp);
        }
    }

    /**
     * The linger is what stops the replication pipeline from being clocked by
     * follower acknowledgements, so a bad value has to fail loudly rather than
     * silently restore the self-clocked behaviour.
     */
    @Test
    void ingressHardCapIsRequiredAndIndependentOfSMax() throws IOException {
        ExperimentConfig config = ExperimentConfig.load(REPO_CONFIG);
        // Three times sMax. The ratio bounds the largest utilization the price
        // controller can observe, since in-flight cannot exceed the cap, so it
        // decides whether the controller can reach uTarget at all: at 18.75x
        // it saturated at lambdaMax under overload, and at 1.5x the cap did all
        // the admission while the price sat on its floor.
        // The shipping local values on the Utah c6525-25g nodes (CALIBRATION.md, "Hardware").
        assertEquals(9600, config.server.ingressHardCapInFlight);
        assertEquals(3200, config.server.sMax);
        assertEquals(3 * config.server.sMax, config.server.ingressHardCapInFlight, 1e-9);
        String stripped = Files.readString(REPO_CONFIG)
                .replaceAll("(?m)^  ingressHardCapInFlight: .*\n", "");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, stripped);
            assertTrue(assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.load(tmp))
                    .getMessage().contains("server.ingressHardCapInFlight"));
        } finally {
            Files.delete(tmp);
        }
    }

    /**
     * Which store a run drove must be stated, never inferred: a generated config
     * that dropped this line once produced a full mongod calibration ladder run
     * against the Java store, with every other key valid and the numbers
     * plausible enough to analyse.
     */
    @Test
    void transportIsRequiredAndNotDefaulted() throws IOException {
        assertEquals("framed", ExperimentConfig.load(REPO_CONFIG).transport);
        String stripped = Files.readString(REPO_CONFIG)
                .replaceAll("(?m)^transport: .*\n", "");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, stripped);
            assertTrue(assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.load(tmp))
                    .getMessage().contains("transport is required"));
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void replicationLingerIsRequiredAndBounded() throws IOException {
        assertEquals(5.0, ExperimentConfig.load(REPO_CONFIG).server.replicationLingerMs, 1e-9);
        // 0 is legal and means "no linger" - the pre-fix behaviour, kept for A/B runs.
        assertEquals(0.0, loadWithLinger("0").server.replicationLingerMs, 1e-9);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> loadWithLinger("-1"))
                .getMessage().contains("non-negative"));
        // Past the heartbeat there is no batching left to win and every majority
        // write pays the wait.
        assertTrue(assertThrows(IllegalArgumentException.class, () -> loadWithLinger("25"))
                .getMessage().contains("must not exceed 20 ms"));
        // A missing key must not fall back to a default.
        String stripped = Files.readString(REPO_CONFIG)
                .replaceAll("(?m)^  replicationLingerMs: .*\n", "");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, stripped);
            assertTrue(assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.load(tmp))
                    .getMessage().contains("server.replicationLingerMs"));
        } finally {
            Files.delete(tmp);
        }
    }

    private static ExperimentConfig loadWithLinger(String value) throws IOException {
        String yaml = Files.readString(REPO_CONFIG)
                .replaceAll("(?m)^  replicationLingerMs: .*$", "  replicationLingerMs: " + value);
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            return ExperimentConfig.load(tmp);
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void mongodbServerHooksDefaultOnAndOffOnlyForClientDecidedModes() throws IOException {
        assertTrue(Files.exists(MONGODB_CONFIG), "repo config_global_mongodb.yaml must exist");
        String base = Files.readString(MONGODB_CONFIG);
        ExperimentConfig config = ExperimentConfig.load(MONGODB_CONFIG);
        assertTrue(config.mongodbServerHooks(), "the switch defaults to on");
        assertTrue(base.contains("\n  serverHooks: true\n"), "the repo config states the default explicitly");

        String off = base.replace("\n  serverHooks: true\n", "\n  serverHooks: false\n");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, off);
            assertEquals(false, ExperimentConfig.load(tmp).mongodbServerHooks(), "weakest may run without the hooks");

            Files.writeString(tmp, off.replaceFirst("(?m)^mode: weakest", "mode: chameleon"));
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("serverHooks"), e.getMessage());

            Files.writeString(tmp, off.replaceFirst("(?m)^mode: weakest", "mode: pileus"));
            e = assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("pileus"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void mongodbPoolWaitTimeoutIsRequiredAndPositive() throws IOException {
        String base = Files.readString(MONGODB_CONFIG);
        assertTrue(base.contains("\n  poolWaitTimeoutMs: 1000\n"), "the repo config sets the pool wait");
        assertEquals(1000, ExperimentConfig.load(MONGODB_CONFIG).mongodb.poolWaitTimeoutMs);
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, base.replace("\n  poolWaitTimeoutMs: 1000\n", "\n"));
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("poolWaitTimeoutMs"), e.getMessage());

            Files.writeString(tmp, base.replace("\n  poolWaitTimeoutMs: 1000\n", "\n  poolWaitTimeoutMs: 0\n"));
            e = assertThrows(IllegalArgumentException.class, () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("poolWaitTimeoutMs"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    private static final Path SCALE_9 = Path.of("..", "config_scale_9.yaml");
    private static final Path SCALE_MONGODB_9 = Path.of("..", "config_scale_mongodb_9.yaml");

    @Test
    void theWriteMajorityFollowsTheStore() {
        assertEquals(5, ExperimentConfig.load(SCALE_9).writeMajority(), "Raft: 9/2 + 1");
        assertEquals(4, ExperimentConfig.load(SCALE_MONGODB_9).writeMajority(),
                "mongod: nine members but at most seven voters, and a majority write is a majority of the voters");
        assertEquals(3, ExperimentConfig.load(MONGODB_CONFIG).writeMajority(), "five members, five voters");
    }

    @Test
    void mongodWriteRungsAreBoundedByTheVotingMajority() throws IOException {
        String yaml = Files.readString(SCALE_MONGODB_9).replace("concern: 4\n", "concern: 5\n");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("voting members"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    @Test
    void mongodPreferredLeaderMustBeAVotingMember() throws IOException {
        String yaml = Files.readString(SCALE_MONGODB_9).replace("\n  preferredLeaderId: 0\n", "\n  preferredLeaderId: 8\n");
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains("non-voting"), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    private static final Path FAILURE_LEADER = Path.of("..", "config_failure_leader_global.yaml");

    private static void assertRejected(String yaml, String fragment) throws IOException {
        Path tmp = Files.createTempFile("config-test", ".yaml");
        try {
            Files.writeString(tmp, yaml);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ExperimentConfig.load(tmp));
            assertTrue(e.getMessage().contains(fragment), e.getMessage());
        } finally {
            Files.delete(tmp);
        }
    }

    /** The leader failure config with its schedule replaced by the given event lines. */
    private static String withSchedule(String base, String schedule) {
        int a = base.indexOf("failureSchedule:");
        int b = base.indexOf("\n\n", a);
        return base.substring(0, a) + "failureSchedule:\n" + schedule + base.substring(b);
    }

    @Test
    void theFailureConfigsLoadAndTheScheduleIsEmptyEverywhereElse() {
        ExperimentConfig leader = ExperimentConfig.load(FAILURE_LEADER);
        assertEquals(4, leader.failureSchedule.size());
        assertEquals(150, leader.failureSchedule.get(0).atSeconds);
        assertEquals("LEADER", leader.failureSchedule.get(0).target);
        assertEquals("recover", leader.failureSchedule.get(2).action);
        assertEquals(660, leader.workload.phases.stream().mapToLong(p -> p.durationSeconds).sum());
        assertTrue(ExperimentConfig.load(REPO_CONFIG).failureSchedule.isEmpty());
        assertTrue(ExperimentConfig.load(MONGODB_CONFIG).failureSchedule.isEmpty());
    }

    @Test
    void failureScheduleIsRejectedOnMongod() throws IOException {
        String yaml = Files.readString(MONGODB_CONFIG).replace("failureSchedule: []\n",
                "failureSchedule:\n  - { atSeconds: 150, action: fail, target: LEADER }\n");
        assertRejected(yaml, "failureSchedule is not supported with transport: mongodb");
    }

    @Test
    void failureScheduleMustKeepAMajorityAlive() throws IOException {
        String base = Files.readString(FAILURE_LEADER);
        assertRejected(withSchedule(base, """
                  - { atSeconds: 150, action: fail, target: LEADER }
                  - { atSeconds: 270, action: fail, target: FOLLOWER }
                  - { atSeconds: 300, action: fail, target: FOLLOWER }
                """), "can lose at most 2");
    }

    @Test
    void failureScheduleStepsMustBeOrderedAndRecoverSomething() throws IOException {
        String base = Files.readString(FAILURE_LEADER);
        assertRejected(withSchedule(base, "  - { atSeconds: 150, action: recover }\n"), "nothing is failed");
        assertRejected(withSchedule(base, """
                  - { atSeconds: 270, action: fail, target: LEADER }
                  - { atSeconds: 150, action: fail, target: FOLLOWER }
                """), "strictly increasing");
        assertRejected(withSchedule(base, "  - { atSeconds: 660, action: fail, target: LEADER }\n"),
                "inside the workload");
        assertRejected(withSchedule(base, "  - { atSeconds: 150, action: fail }\n"), "LEADER or FOLLOWER");
        assertRejected(withSchedule(base, """
                  - { atSeconds: 150, action: fail, target: LEADER }
                  - { atSeconds: 300, action: recover, target: LEADER }
                """), "must be absent for a recover");
        assertRejected(withSchedule(base, "  - { atSeconds: 150, action: crash, target: LEADER }\n"),
                "fail or recover");
    }
}
