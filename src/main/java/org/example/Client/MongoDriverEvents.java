package org.example.Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;

/**
 * Driver-side accounting of what the MongoDB driver did to its view of the
 * cluster during a run: nodes marked unknown (and the exception that did it),
 * topology membership changes, pool clears, checkout failures by reason, and
 * heartbeat failures and durations. One instance per site client; the summary
 * is printed with the run's totals so a run says why its calls failed.
 */
final class MongoDriverEvents implements ServerListener, ClusterListener, ConnectionPoolListener,
        ServerMonitorListener {

    private static final int SAMPLES = 3;

    private final String site;
    private final long startNanos = System.nanoTime();
    private final Map<String, AtomicLong> markedUnknown = new ConcurrentHashMap<>();
    private final List<String> markedUnknownSamples = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong membershipChanges = new AtomicLong();
    private final List<String> membershipSamples = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, AtomicLong> poolCleared = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> checkoutFailed = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> heartbeatFailed = new ConcurrentHashMap<>();
    private final List<String> heartbeatFailedSamples = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong heartbeats = new AtomicLong();
    private final AtomicLong slowestHeartbeatMs = new AtomicLong();

    MongoDriverEvents(String site) {
        this.site = site;
    }

    private String at() {
        return String.format("t+%.1fs", (System.nanoTime() - startNanos) / 1e9);
    }

    private static void sample(List<String> samples, String text) {
        if (samples.size() < SAMPLES) {
            samples.add(text);
        }
    }

    @Override
    public void serverDescriptionChanged(ServerDescriptionChangedEvent event) {
        ServerDescription now = event.getNewDescription();
        ServerDescription before = event.getPreviousDescription();
        if (now.getType() == ServerType.UNKNOWN && before.getType() != ServerType.UNKNOWN) {
            String address = event.getServerId().getAddress().toString();
            markedUnknown.computeIfAbsent(address, k -> new AtomicLong()).incrementAndGet();
            Throwable cause = now.getException();
            sample(markedUnknownSamples, at() + " " + address + " " + before.getType() + " -> UNKNOWN, state "
                    + now.getState() + ", exception: " + (cause == null ? "none" : cause.toString()));
        }
    }

    @Override
    public void clusterDescriptionChanged(ClusterDescriptionChangedEvent event) {
        List<String> before = addresses(event.getPreviousDescription().getServerDescriptions());
        List<String> now = addresses(event.getNewDescription().getServerDescriptions());
        if (!before.equals(now)) {
            membershipChanges.incrementAndGet();
            sample(membershipSamples, at() + " members " + before + " -> " + now);
        }
    }

    private static List<String> addresses(List<ServerDescription> descriptions) {
        List<String> out = new ArrayList<>();
        for (ServerDescription d : descriptions) {
            out.add(d.getAddress().toString());
        }
        Collections.sort(out);
        return out;
    }

    @Override
    public void connectionPoolCleared(ConnectionPoolClearedEvent event) {
        poolCleared.computeIfAbsent(event.getServerId().getAddress().toString(), k -> new AtomicLong())
                .incrementAndGet();
    }

    @Override
    public void connectionCheckOutFailed(ConnectionCheckOutFailedEvent event) {
        checkoutFailed.computeIfAbsent(event.getReason().toString(), k -> new AtomicLong()).incrementAndGet();
        // A checkout that gave up waited at least this long; dropping it would bias the
        // estimate downwards exactly when the queue is at its worst.
        Pool p = pool(event.getServerId().getAddress().toString());
        p.waiting.decrementAndGet();
        p.sampleWait(event.getElapsedTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event) {
        heartbeats.incrementAndGet();
        slowestHeartbeatMs.accumulateAndGet(event.getElapsedTime(TimeUnit.MILLISECONDS), Math::max);
    }

    @Override
    public void serverHeartbeatFailed(ServerHeartbeatFailedEvent event) {
        String address = event.getConnectionId().getServerId().getAddress().toString();
        heartbeatFailed.computeIfAbsent(address, k -> new AtomicLong()).incrementAndGet();
        sample(heartbeatFailedSamples, at() + " " + address + " after " + event.getElapsedTime(TimeUnit.MILLISECONDS)
                + " ms: " + event.getThrowable());
    }

    // ===== Per-pool queueing state =====
    //
    // A pooled connection is held for one round trip, so a saturated pool queues
    // checkouts. That wait is ours, not the node's: it must be kept out of the round
    // trip estimate the scorer reads, and a request must not enter a queue longer than
    // its own deadline. Both need a live view per pool, which the driver only exposes
    // through these events.
    private static final double WAIT_EWMA_ALPHA = 0.05;

    private static final class Pool {
        final AtomicLong waitMsBits = new AtomicLong(Double.doubleToLongBits(0.0));
        final AtomicInteger size = new AtomicInteger();
        /** Checkouts started and not yet satisfied or given up: the queue, right now. */
        final AtomicInteger waiting = new AtomicInteger();

        void sampleWait(double ms) {
            waitMsBits.accumulateAndGet(Double.doubleToLongBits(ms), (prevBits, sampleBits) -> {
                double prev = Double.longBitsToDouble(prevBits);
                double sample = Double.longBitsToDouble(sampleBits);
                return Double.doubleToLongBits(prev + WAIT_EWMA_ALPHA * (sample - prev));
            });
        }

        double waitMs() {
            return Double.longBitsToDouble(waitMsBits.get());
        }
    }

    private final Map<String, Pool> pools = new ConcurrentHashMap<>();

    private Pool pool(String address) {
        return pools.computeIfAbsent(address, k -> new Pool());
    }

    /**
     * Recent mean time a checkout waited for a connection to this address, in ms.
     * Smoothed per pool rather than tracked per request: every checkout queued on a
     * pool at the same moment waits about the same, so the queue is the right
     * granularity and the driver gives no per-request handle to correlate against.
     */
    double checkoutWaitMs(String address) {
        Pool p = pools.get(address);
        return p == null ? 0.0 : p.waitMs();
    }

    /**
     * Whether this pool holds its full complement of connections, so a checkout can
     * only wait for one to come free rather than for a new one to open. Below this a
     * wait is productive (the pool is still growing) and must not be refused.
     */
    boolean poolSaturated(String address, int maxPoolSize) {
        Pool p = pools.get(address);
        return p != null && p.size.get() >= maxPoolSize;
    }

    /**
     * Checkouts currently queued on this pool. Read alongside checkoutWaitMs before
     * refusing anything: the smoothed wait says how long a queue has been taking, and
     * this says whether there is a queue at all. Without it the smoothed wait latches.
     * A pool whose requests are all refused performs no checkouts, so it produces no
     * fresh samples, so a stale high mean keeps refusing and nothing can ever
     * contradict it. This count falls to zero the moment we stop queueing on the pool,
     * which is what lets a pool recover.
     */
    int queueDepth(String address) {
        Pool p = pools.get(address);
        return p == null ? 0 : Math.max(0, p.waiting.get());
    }

    @Override
    public void connectionCheckOutStarted(ConnectionCheckOutStartedEvent event) {
        pool(event.getServerId().getAddress().toString()).waiting.incrementAndGet();
    }

    @Override
    public void connectionCheckedOut(ConnectionCheckedOutEvent event) {
        Pool p = pool(event.getConnectionId().getServerId().getAddress().toString());
        p.waiting.decrementAndGet();
        p.sampleWait(event.getElapsedTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public void connectionCreated(ConnectionCreatedEvent event) {
        pool(event.getConnectionId().getServerId().getAddress().toString()).size.incrementAndGet();
    }

    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        pool(event.getConnectionId().getServerId().getAddress().toString()).size.decrementAndGet();
    }

    boolean quiet() {
        return markedUnknown.isEmpty() && membershipChanges.get() == 0 && poolCleared.isEmpty()
                && checkoutFailed.isEmpty() && heartbeatFailed.isEmpty();
    }

    /** Multi-line summary for the run log; empty when nothing notable happened. */
    String summary() {
        if (quiet()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Driver events at site ").append(site).append(": heartbeats ").append(heartbeats.get())
                .append(" (slowest ").append(slowestHeartbeatMs.get()).append(" ms)\n");
        appendCounts(sb, "  nodes marked UNKNOWN", markedUnknown, markedUnknownSamples);
        if (membershipChanges.get() > 0) {
            sb.append("  topology membership changes: ").append(membershipChanges.get()).append('\n');
            for (String s : membershipSamples) {
                sb.append("      ").append(s).append('\n');
            }
        }
        appendCounts(sb, "  pools cleared", poolCleared, List.of());
        appendCounts(sb, "  checkouts failed by reason", checkoutFailed, List.of());
        appendCounts(sb, "  heartbeats failed", heartbeatFailed, heartbeatFailedSamples);
        return sb.toString();
    }

    private static void appendCounts(StringBuilder sb, String title, Map<String, AtomicLong> counts,
            List<String> samples) {
        if (counts.isEmpty()) {
            return;
        }
        sb.append(title).append(": ");
        List<String> keys = new ArrayList<>(counts.keySet());
        Collections.sort(keys);
        for (String k : keys) {
            sb.append(k).append('=').append(counts.get(k).get()).append(' ');
        }
        sb.append('\n');
        for (String s : samples) {
            sb.append("      ").append(s).append('\n');
        }
    }
}
