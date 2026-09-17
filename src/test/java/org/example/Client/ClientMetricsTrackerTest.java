package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Transport failure descriptions fold into one key per cause. */
class ClientMetricsTrackerTest {

    @Test
    void causeKeyCollapsesDigitsAndBoundsLength() {
        String a = ClientMetricsTracker.causeKey("com.mongodb.MongoTimeoutException: Timed out after 1000 ms while"
                + " waiting for a connection to server 127.0.1.3:27017");
        String b = ClientMetricsTracker.causeKey("com.mongodb.MongoTimeoutException: Timed out after 999 ms while"
                + " waiting for a connection to server 127.0.1.5:27017");
        assertEquals(a, b, "addresses and timings do not split a cause");
        assertEquals("com.mongodb.MongoTimeoutException: Timed out after # ms while waiting for a connection to"
                + " server #.#.#.#:#", a);
        assertEquals("(no description)", ClientMetricsTracker.causeKey(null));
        assertEquals("(no description)", ClientMetricsTracker.causeKey("  "));
        assertEquals(160, ClientMetricsTracker.causeKey("x".repeat(500)).length());
    }

    @Test
    void keepsTheFirstFullDescriptionsPerCause() {
        for (int i = 0; i < 5; i++) {
            ClientMetricsTracker.recordTransportError("test cause " + i + ": detail " + i);
        }
        var samples = ClientMetricsTracker.transportErrorSamples(ClientMetricsTracker.causeKey("test cause 0: detail 0"));
        assertEquals(2, samples.size(), "two samples per cause");
        assertEquals("test cause 0: detail 0", samples.get(0));
        assertEquals("test cause 1: detail 1", samples.get(1));
    }
}
