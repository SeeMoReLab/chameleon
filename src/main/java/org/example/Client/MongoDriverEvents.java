package org.example.Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
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
