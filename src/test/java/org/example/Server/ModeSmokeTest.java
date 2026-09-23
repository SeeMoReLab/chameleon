package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.example.Client.ClientMode;
import org.example.Client.KvSessionClient;
import org.example.Utility.RungScorer;
import org.example.raft.ReadLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * One smoke pass per experiment arm: a real cluster, the real client in the
 * arm's mode, a few hundred interleaved writes and causal-chasing reads.
 * Every request must resolve and the session guarantees must hold - the
 * client-side grader asserts them in every mode.
 */
class ModeSmokeTest {

    private static final List<RungScorer.Rung> READ_SLA = List.of(
            new RungScorer.Rung(ReadLevel.LINEARIZABLE.getNumber(), 2000, 10),
            new RungScorer.Rung(ReadLevel.CAUSAL_MAJORITY.getNumber(), 1000, 6),
            new RungScorer.Rung(ReadLevel.EVENTUAL_LOCAL.getNumber(), 500, 1));
    private static final List<RungScorer.Rung> WRITE_SLA = List.of(
            new RungScorer.Rung(2, 2000, 5),
            new RungScorer.Rung(1, 1000, 2));

    @BeforeAll
    static void setUp() {
        MeasurementPlane.applyEconomics(1000, 100, 0.85, 1.0, 0.0001, 1.0e6);
        SlaRegistry.registerReadSla(70, 1, READ_SLA);
        SlaRegistry.registerWriteSla(70, 1, WRITE_SLA);
    }

    @AfterAll
    static void tearDown() {
        KvClientService.setChameleonDecisionForTest(true);
    }

    private void smoke(ClientMode mode, int basePort) throws Exception {
        KvClientService.setChameleonDecisionForTest(mode.chameleonDecision());
        try (TestCluster cluster = new TestCluster(basePort)) {
            cluster.awaitLeader(15_000);
            try (KvSessionClient client = new KvSessionClient(70,
                    List.of("localhost", "localhost", "localhost"), cluster.clientBasePort, mode, 32, 3, 8_000,
                    Map.of(1, READ_SLA), Map.of(1, WRITE_SLA), 0.05, false, true, 0.9)) {

                for (int i = 0; i < 200; i++) {
                    String key = "m" + (i % 8);
                    client.sendWrite(key, "v" + i, 1);
                    client.sendRead(key, 1);
                }
                long deadline = System.currentTimeMillis() + 20_000;
                while (client.pendingCount() > 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                }
                assertEquals(0, client.pendingCount(), mode + ": all requests must resolve");
            }
        } finally {
            KvClientService.setChameleonDecisionForTest(true);
        }
    }

    @Test
    void chameleon() throws Exception {
        smoke(ClientMode.CHAMELEON, 20000);
    }

    @Test
    void chameleonPileus() throws Exception {
        smoke(ClientMode.CHAMELEON_PILEUS, 20100);
    }

    @Test
    void pileus() throws Exception {
        smoke(ClientMode.PILEUS, 20200);
    }

    @Test
    void strongest() throws Exception {
        smoke(ClientMode.STRONGEST, 20300);
    }

    @Test
    void weakest() throws Exception {
        smoke(ClientMode.WEAKEST, 20400);
    }
}
