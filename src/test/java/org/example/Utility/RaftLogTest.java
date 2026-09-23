package org.example.Utility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The log's indices are logical and survive compaction; only the retained
 * range moves. Everything the consensus code asks of the log across the
 * compaction boundary is pinned here.
 */
class RaftLogTest {

    private static LogEntry e(int index, int term) {
        return new LogEntry(index, term, "k" + index, "v" + index, "op" + index);
    }

    private static RaftLog logOf(int n, int term) {
        RaftLog log = new RaftLog();
        for (int i = 0; i < n; i++) {
            log.append(e(i, term));
        }
        return log;
    }

    @Test
    void emptyLogHasNoIndices() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.firstIndex());
        assertEquals(-1, log.lastIncludedIndex());
        assertEquals(0, log.nextIndex());
        assertEquals(-1, log.lastIndex());
        assertEquals(-1, log.lastTerm());
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(-1, -1));
        assertFalse(log.checkIfPrevLogIndexHasPrevLogTerm(0, 1));
    }

    @Test
    void compactionKeepsLogicalIndicesAndRemembersTheBoundaryTerm() {
        RaftLog log = logOf(10, 3);
        log.compactThrough(6);

        assertEquals(7, log.firstIndex());
        assertEquals(6, log.lastIncludedIndex());
        assertEquals(3, log.lastIncludedTerm());
        assertEquals(3, log.retainedSize());
        assertEquals(10, log.nextIndex());
        assertEquals(9, log.lastIndex());
        assertEquals(3, log.lastTerm());
        assertEquals("v7", log.get(7).value);
        assertEquals("v9", log.get(9).value);
        assertTrue(log.contains(7));
        assertFalse(log.contains(6));
        assertFalse(log.contains(10));

        // Reading into the compacted prefix is a bug, not a miss.
        assertThrows(IllegalArgumentException.class, () -> log.get(6));
        assertThrows(IllegalArgumentException.class, () -> log.get(10));

        // The boundary term is still answerable; anything older is not.
        assertEquals(3, log.termAt(6));
        assertEquals(3, log.termAt(7));
        assertThrows(IllegalArgumentException.class, () -> log.termAt(5));

        // Appending continues at the logical next index.
        log.append(e(10, 4));
        assertEquals(11, log.nextIndex());
        assertEquals(4, log.lastTerm());
        assertThrows(IllegalStateException.class, () -> log.append(e(15, 4)));
    }

    @Test
    void consistencyCheckAcrossTheBoundary() {
        RaftLog log = logOf(10, 3);
        log.compactThrough(6);

        // Below the boundary: committed everywhere, matches without a term.
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(2, 99));
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(-1, -1));
        // At the boundary: the remembered term decides.
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(6, 3));
        assertFalse(log.checkIfPrevLogIndexHasPrevLogTerm(6, 2));
        // Retained: the entry's own term.
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(8, 3));
        assertFalse(log.checkIfPrevLogIndexHasPrevLogTerm(8, 4));
        // Beyond the end: no entry to match.
        assertFalse(log.checkIfPrevLogIndexHasPrevLogTerm(10, 3));
    }

    @Test
    void rangesClampToTheRetainedWindow() {
        RaftLog log = logOf(10, 1);
        log.compactThrough(3);
        List<LogEntry> range = log.entriesInRange(0, 6);
        assertEquals(2, range.size());
        assertEquals(4, range.get(0).index);
        assertEquals(5, range.get(1).index);
        assertTrue(log.entriesInRange(0, 4).isEmpty());
        assertEquals(6, log.entriesInRange(4, 100).size());
    }

    @Test
    void truncationNeverReachesIntoTheCompactedPrefix() {
        RaftLog log = logOf(10, 1);
        log.compactThrough(4);
        log.truncateAfter(8);
        assertEquals(8, log.nextIndex());
        log.truncateAfter(5);
        assertEquals(5, log.nextIndex());
        assertEquals(0, log.retainedSize());
        assertEquals(4, log.lastIndex());
        assertEquals(1, log.lastTerm());
        assertThrows(IllegalStateException.class, () -> log.truncateAfter(4));
    }

    @Test
    void compactionIsMonotonicAndIdempotentAtTheBoundary() {
        RaftLog log = logOf(10, 1);
        log.compactThrough(4);
        log.compactThrough(4);
        assertEquals(5, log.firstIndex());
        assertThrows(IllegalArgumentException.class, () -> log.compactThrough(2));
        assertThrows(IllegalArgumentException.class, () -> log.compactThrough(10));
        log.compactThrough(9);
        assertEquals(0, log.retainedSize());
        assertEquals(10, log.nextIndex());
        assertEquals(1, log.lastTerm());
    }

    @Test
    void snapshotResetDiscardsEverythingAndMovesTheBoundary() {
        RaftLog log = logOf(5, 1);
        log.resetToSnapshot(42, 7);
        assertEquals(43, log.firstIndex());
        assertEquals(42, log.lastIndex());
        assertEquals(7, log.lastTerm());
        assertEquals(0, log.retainedSize());
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(42, 7));
        assertTrue(log.checkIfPrevLogIndexHasPrevLogTerm(10, 1));
        assertFalse(log.checkIfPrevLogIndexHasPrevLogTerm(43, 7));
        log.append(e(43, 8));
        assertEquals(44, log.nextIndex());
    }
}
