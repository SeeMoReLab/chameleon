package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;

/**
 * The per-pool queueing view the transport reads: how long checkouts are waiting
 * and whether the pool can still grow. Both decide whether a request may queue and
 * what is subtracted from the round trip estimate.
 */
class MongoDriverEventsTest {

    private static final String ADDRESS = "127.0.1.3:27017";
    private static final ServerId SERVER_ID =
            new ServerId(new ClusterId("c"), new ServerAddress("127.0.1.3", 27017));

    private static ConnectionId connectionId() {
        return new ConnectionId(SERVER_ID);
    }

    private static void checkedOut(MongoDriverEvents events, long waitedMs) {
        events.connectionCheckedOut(new ConnectionCheckedOutEvent(connectionId(), 1L,
                TimeUnit.MILLISECONDS.toNanos(waitedMs)));
    }

    @Test
    void anUnseenPoolReportsNoQueueAndIsNotSaturated() {
        MongoDriverEvents events = new MongoDriverEvents("virginia");
        assertEquals(0.0, events.checkoutWaitMs(ADDRESS));
        assertFalse(events.poolSaturated(ADDRESS, 1000), "nothing is known about a pool with no events yet");
        assertTrue(events.quiet(), "pool bookkeeping alone is not a reportable incident");
    }

    @Test
    void checkoutWaitFollowsRecentSamples() {
        MongoDriverEvents events = new MongoDriverEvents("virginia");
        for (int i = 0; i < 500; i++) {
            checkedOut(events, 400);
        }
        assertEquals(400.0, events.checkoutWaitMs(ADDRESS), 1.0, "converges on a steady queue");

        for (int i = 0; i < 500; i++) {
            checkedOut(events, 0);
        }
        assertEquals(0.0, events.checkoutWaitMs(ADDRESS), 1.0, "and comes back down when the queue drains");
    }

    @Test
    void aGivenUpCheckoutStillCountsAsWaiting() {
        MongoDriverEvents events = new MongoDriverEvents("virginia");
        for (int i = 0; i < 500; i++) {
            events.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(SERVER_ID, 1L,
                    ConnectionCheckOutFailedEvent.Reason.TIMEOUT, TimeUnit.MILLISECONDS.toNanos(1000)));
        }
        assertEquals(1000.0, events.checkoutWaitMs(ADDRESS), 5.0,
                "timeouts are the longest waits; dropping them would hide the worst queue");
    }

    @Test
    void saturationTracksLiveConnectionCount() {
        MongoDriverEvents events = new MongoDriverEvents("virginia");
        for (int i = 0; i < 3; i++) {
            events.connectionCreated(new ConnectionCreatedEvent(connectionId()));
        }
        assertFalse(events.poolSaturated(ADDRESS, 4), "still able to open another connection");
        events.connectionCreated(new ConnectionCreatedEvent(connectionId()));
        assertTrue(events.poolSaturated(ADDRESS, 4), "full: a checkout can only wait for one to come free");
        events.connectionClosed(new ConnectionClosedEvent(connectionId(), ConnectionClosedEvent.Reason.ERROR));
        assertFalse(events.poolSaturated(ADDRESS, 4), "a closed connection leaves room to grow again");
    }

    @Test
    void refusalNeedsBothAFullPoolAndAQueueLongerThanTheDeadline() {
        MongoDriverEvents events = new MongoDriverEvents("virginia");
        for (int i = 0; i < 500; i++) {
            checkedOut(events, 400);
        }
        assertFalse(MongoTransport.refuseForQueue(events, ADDRESS, 4, 40),
                "a growing pool is never refused: waiting for it to open a connection is productive");

        for (int i = 0; i < 4; i++) {
            events.connectionCreated(new ConnectionCreatedEvent(connectionId()));
        }
        assertTrue(MongoTransport.refuseForQueue(events, ADDRESS, 4, 40),
                "400 ms of queue cannot serve 40 ms of deadline");
        assertFalse(MongoTransport.refuseForQueue(events, ADDRESS, 4, 800),
                "a patient request may still queue");
    }
}
