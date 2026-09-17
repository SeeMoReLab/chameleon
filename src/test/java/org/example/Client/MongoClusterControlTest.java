package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/** The occupancy row built from stock serverStatus fields when the server hooks are off. */
class MongoClusterControlTest {

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
