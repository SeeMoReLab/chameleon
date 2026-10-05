package org.example.Utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.example.raft.LogEntryProto;

/**
 * The Raft log: an in-memory, bounded list of KV write entries.
 *
 * Indices are logical and never change: entry i is the i-th write ever
 * appended, 0-based. What changes is how much of the prefix is retained. The
 * owner compacts committed entries away once they are no longer needed for
 * replication (ServerImpl.compactIfDue), so the list holds indices
 * [firstIndex, nextIndex) and everything below firstIndex has been folded into
 * the committed KV state. The log remembers the term of the last compacted
 * entry, lastIncludedTerm, because the AppendEntries consistency check may
 * name that index as its prevLogIndex.
 *
 * Reading a compacted index is a programming error, not a miss, and throws.
 * A log that grew without bound filled a 30 GiB heap after about 145 million
 * writes and put every node into multi-second GC pauses, which is what this
 * bound exists to prevent (CALIBRATION.md).
 *
 * Thread safe via an internal read-write lock; the owning ServerImpl
 * additionally orders append/truncate/compact against its own consensus lock.
 * The next index alone is also published through a volatile, written by every
 * mutation while it holds the write lock, so nextIndex() and lastIndex() never
 * take the lock. Request threads read the last index on every reply; under the
 * lock those reads held off the leader's appends, each of which runs inside the
 * consensus write lock, and the queue that formed behind the appends stalled
 * the replication acknowledgements (a lock convoy at 27 nodes).
 */
public class RaftLog {
    private final ArrayList<LogEntry> entries = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // Logical index of entries.get(0). Entries below it are compacted.
    private int firstIndex = 0;
    // Term of the entry at firstIndex - 1; -1 while nothing has been compacted.
    private int lastIncludedTerm = -1;
    // Always firstIndex + entries.size(); written only under the write lock.
    // Volatile so a reader that sees index i also sees what the writer did
    // before publishing it (the owner applies the KV view before appending).
    private volatile int nextIndex = 0;

    /** Logical index of the first retained entry (0 while nothing is compacted). */
    public int firstIndex() {
        lock.readLock().lock();
        try {
            return firstIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Index of the last compacted entry, -1 while nothing is compacted. */
    public int lastIncludedIndex() {
        lock.readLock().lock();
        try {
            return firstIndex - 1;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int lastIncludedTerm() {
        lock.readLock().lock();
        try {
            return lastIncludedTerm;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Index the next appended entry will get: one past the last entry, retained or compacted. Lock-free. */
    public int nextIndex() {
        return nextIndex;
    }

    /** Index of the last entry ever appended (retained or compacted); -1 for an empty log. */
    public int lastIndex() {
        return nextIndex() - 1;
    }

    /** Term of the last entry ever appended; falls back to the compacted tail; -1 for an empty log. */
    public int lastTerm() {
        lock.readLock().lock();
        try {
            return entries.isEmpty() ? lastIncludedTerm : entries.get(entries.size() - 1).term;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Number of retained entries, for observability. */
    public int retainedSize() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** True when the entry at this index is retained (not compacted, not beyond the end). */
    public boolean contains(int index) {
        lock.readLock().lock();
        try {
            return index >= firstIndex && index < firstIndex + entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Append the leader's own entry; its index must be exactly the next index. */
    public void append(LogEntry entry) {
        lock.writeLock().lock();
        try {
            appendLocked(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Append entries received from the leader (already consistency-checked); returns them. */
    public List<LogEntry> appendProtoEntries(List<LogEntryProto> protoEntries) {
        lock.writeLock().lock();
        try {
            List<LogEntry> appended = new ArrayList<>(protoEntries.size());
            for (LogEntryProto proto : protoEntries) {
                LogEntry entry = new LogEntry(proto.getLogIndex(), proto.getTerm(), proto.getKey(),
                        proto.getValue(), proto.getOpId());
                appendLocked(entry);
                appended.add(entry);
            }
            return appended;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void appendLocked(LogEntry entry) {
        int expected = firstIndex + entries.size();
        if (entry.index != expected) {
            throw new IllegalStateException("log entry index " + entry.index + " appended where " + expected
                    + " was expected (first retained " + firstIndex + ")");
        }
        entries.add(entry);
        publishNextIndexLocked();
    }

    // Every mutation ends here, under the write lock.
    private void publishNextIndexLocked() {
        nextIndex = firstIndex + entries.size();
    }

    /** The retained entry at a logical index; throws for compacted or unwritten indices. */
    public LogEntry get(int index) {
        lock.readLock().lock();
        try {
            return getLocked(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    private LogEntry getLocked(int index) {
        if (index < firstIndex) {
            throw new IllegalArgumentException("log index " + index + " is compacted (first retained is "
                    + firstIndex + ")");
        }
        if (index >= firstIndex + entries.size()) {
            throw new IllegalArgumentException("log index " + index + " is beyond the last entry "
                    + (firstIndex + entries.size() - 1));
        }
        return entries.get(index - firstIndex);
    }

    /**
     * Term of the entry at a logical index. Answers for the last compacted
     * index as well, which is the one AppendEntries may name as prevLogIndex
     * right after a compaction; anything older throws.
     */
    public int termAt(int index) {
        lock.readLock().lock();
        try {
            if (index == firstIndex - 1) {
                return lastIncludedTerm;
            }
            return getLocked(index).term;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Copy of the retained entries in [start, end); clamped to the retained range. */
    public List<LogEntry> entriesInRange(int start, int end) {
        lock.readLock().lock();
        try {
            int from = Math.max(0, start - firstIndex);
            int to = Math.min(end - firstIndex, entries.size());
            if (from >= to) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(from, to));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Remove entries from index (inclusive) to the end. Only retained,
     * uncommitted entries may be truncated; asking to truncate into the
     * compacted prefix would roll back committed state and throws.
     */
    public void truncateAfter(int index) {
        lock.writeLock().lock();
        try {
            if (index < firstIndex) {
                throw new IllegalStateException("truncation at " + index + " would remove compacted entries"
                        + " (first retained is " + firstIndex + ")");
            }
            int from = index - firstIndex;
            if (from < entries.size()) {
                entries.subList(from, entries.size()).clear();
            }
            publishNextIndexLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop every entry at or below lastIncluded, remembering its term. The
     * caller guarantees lastIncluded is committed; the log only checks that
     * the index is retained (or already the compaction boundary) so the term
     * can be recorded.
     */
    public void compactThrough(int lastIncluded) {
        lock.writeLock().lock();
        try {
            if (lastIncluded < firstIndex - 1) {
                throw new IllegalArgumentException("compaction through " + lastIncluded
                        + " is behind the current boundary " + (firstIndex - 1));
            }
            if (lastIncluded == firstIndex - 1) {
                return;
            }
            int term = getLocked(lastIncluded).term;
            entries.subList(0, lastIncluded - firstIndex + 1).clear();
            firstIndex = lastIncluded + 1;
            lastIncludedTerm = term;
            publishNextIndexLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Replace the whole log by a snapshot boundary: nothing retained, the
     * next index is lastIncluded + 1. Used when a follower installs the
     * leader's snapshot; whatever it held is either already in the snapshot
     * or will be resent by the leader.
     */
    public void resetToSnapshot(int lastIncluded, int term) {
        lock.writeLock().lock();
        try {
            entries.clear();
            firstIndex = lastIncluded + 1;
            lastIncludedTerm = term;
            publishNextIndexLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The AppendEntries consistency check. An index below the compaction
     * boundary is committed here, and committed entries are identical on
     * every node (leader completeness), so it matches without a term to
     * compare; the boundary itself compares against the remembered term.
     */
    public boolean checkIfPrevLogIndexHasPrevLogTerm(int prevLogIndex, int prevLogTerm) {
        lock.readLock().lock();
        try {
            if (prevLogIndex < firstIndex - 1) {
                return true;
            }
            if (prevLogIndex == firstIndex - 1) {
                return firstIndex == 0 || lastIncludedTerm == prevLogTerm;
            }
            return prevLogIndex < firstIndex + entries.size() && getLocked(prevLogIndex).term == prevLogTerm;
        } finally {
            lock.readLock().unlock();
        }
    }
}
