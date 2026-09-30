package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;

import org.bson.Document;
import org.example.Utility.ExperimentConfig;
import org.junit.jupiter.api.Test;

/** The parameters pushed to mongod, and the occupancy row built from stock serverStatus fields when the server hooks are off. */
class MongoClusterControlTest {

    private static final Path MONGODB_CONFIG = Path.of("..", "config_global_mongodb.yaml");

    @Test
    void pushesTheHardCapAndThePriceCeilingFromTheConfig() {
        ExperimentConfig config = ExperimentConfig.load(MONGODB_CONFIG);
        Document parameters = MongoClusterControl.serverParameters(config, ClientMode.CHAMELEON);
        assertEquals(config.server.ingressHardCapInFlight, parameters.get("chameleonHardCapInFlight"),
                "mongod used a hard-coded 1.5 x sMax until the cap was pushed; the config's value must win");
        assertEquals(config.chameleon.lambdaMax, parameters.get("chameleonLambdaMax"));
        assertEquals(config.chameleon.lambdaMin, parameters.get("chameleonLambdaMin"));
        assertEquals(config.server.sMax, parameters.get("chameleonSMax"));
        assertEquals(true, parameters.get("chameleonServerDecision"));
        assertEquals(false, MongoClusterControl.serverParameters(config, ClientMode.WEAKEST).get("chameleonServerDecision"));
    }

    @Test
    void ceilingIsPushedBeforeTheFloor() {
        ExperimentConfig config = ExperimentConfig.load(MONGODB_CONFIG);
        List<String> order = List.copyOf(MongoClusterControl.serverParameters(config, ClientMode.CHAMELEON).keySet());
        assertTrue(order.indexOf("chameleonLambdaMax") < order.indexOf("chameleonLambdaMin"),
                "the parameters land one at a time; a ceiling set first can never invert the pair, got " + order);
    }

    private static Document status(Object activeTotal, boolean primary) {
        return new Document("globalLock", new Document("activeClients", new Document("total", activeTotal)))
                .append("repl", new Document("isWritablePrimary", primary).append("secondary", !primary));
    }

    @Test
    void stockRowReadsActiveClientsAsInt32OrInt64AndRoleFromRepl() {
        assertEquals("nan,nan,7.000,7,nan,LEADER", MongoClusterControl.stockOccupancyRow(status(7, true)),
                "8.x releases report activeClients.total as int32");
        assertEquals("nan,nan,12.000,12,nan,FOLLOWER", MongoClusterControl.stockOccupancyRow(status(12L, false)),
                "master reports it as int64");
    }

    @Test
    void stockRowWithoutActiveClientsIsAnExplicitError() {
        assertThrows(IllegalStateException.class,
                () -> MongoClusterControl.stockOccupancyRow(new Document("repl", new Document("secondary", true))));
    }
}
