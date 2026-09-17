package org.example.Server;

import java.util.concurrent.ConcurrentHashMap;

import org.example.Utility.LogEntry;

/**
 * Replicated key-value state machine with two views.
 *
 * The local view is applied when an entry is appended to the log and may roll
 * back on leader change; the committed view is applied when the commit index
 * advances and never rolls back. Eventual/causal-local reads serve from the
 * local view, majority-level reads from the committed view (MongoDB style
 * "local" vs "majority" read concerns on Raft).
 *
 * Every value carries the log index that last modified it. Step 7 of the
 * request path (grading after the fact) compares that index against the commit
 * index and the client's session indices to detect free upgrades.
 *
 * Thread safety: the maps are concurrent so reads never block, but all
 * mutations (apply*, rebuildLocal) must be called while holding the owning
 * ServerImpl's write lock, which is what orders them against log truncation
 * and commit-index advancement.
 */
public final class KvStore {

    /** A value plus the log index of the write that produced it. */
    public record Versioned(String value, long index) {
    }

    private final ConcurrentHashMap<String, Versioned> local = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Versioned> committed = new ConcurrentHashMap<>();

    /** Apply a log entry to the local (may-roll-back) view at append time. */
    public void applyLocal(LogEntry entry) {
        local.put(entry.key, new Versioned(entry.value, entry.index));
    }

    /** Apply a log entry to the committed view when the commit index reaches it. */
    public void applyCommitted(LogEntry entry) {
        committed.put(entry.key, new Versioned(entry.value, entry.index));
    }

    /** Latest locally applied version of the key, or null if never written. */
    public Versioned readLocal(String key) {
        return local.get(key);
    }

    /** Latest majority-committed version of the key, or null if never written. */
    public Versioned readCommitted(String key) {
        return committed.get(key);
    }

    /**
     * Rebuild the local view after log truncation: committed state plus the
     * surviving uncommitted suffix, in log order. The caller passes exactly the
     * entries in (commitIndex, truncation point); entries at or below the
     * commit index are already in the committed view.
     */
    public void rebuildLocal(Iterable<LogEntry> survivingUncommittedSuffix) {
        local.clear();
        local.putAll(committed);
        for (LogEntry entry : survivingUncommittedSuffix) {
            applyLocal(entry);
        }
    }
}
