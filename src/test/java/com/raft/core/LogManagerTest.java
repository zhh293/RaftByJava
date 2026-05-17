package com.raft.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogManagerTest {
    private LogManager log;

    @BeforeEach
    void setUp() {
        log = new LogManager();
    }

    @Test
    void emptyLogHasSentinel() {
        LogEntry last = log.getLast();
        assertEquals(0, last.getTerm());
        assertEquals(0, last.getIndex());
    }

    @Test
    void lastLogIndexIsZeroWhenEmpty() {
        assertEquals(0, log.lastLogIndex());
    }

    @Test
    void appendAssignsCorrectIndexAndTerm() {
        LogEntry e = log.append(1, "set x=1");
        assertEquals(1, e.getIndex());
        assertEquals(1, e.getTerm());
        assertEquals("set x=1", e.getCommand());
    }

    @Test
    void appendIncrementsIndex() {
        log.append(1, "set x=1");
        LogEntry e2 = log.append(1, "set x=2");
        assertEquals(2, e2.getIndex());
    }

    @Test
    void getReturnsNullForOutOfRange() {
        assertNull(log.get(-1));
        assertNull(log.get(999));
    }

    @Test
    void getReturnsSentinelForIndexZero() {
        LogEntry e = log.get(0);
        assertEquals(0, e.getTerm());
        assertEquals(0, e.getIndex());
        assertTrue(e.isNoOp());
    }

    @Test
    void truncateFromRemovesEntries() {
        log.append(1, "a");
        log.append(1, "b");
        log.append(2, "c");
        log.truncateFrom(2);
        assertEquals(1, log.lastLogIndex());
        assertEquals("a", log.get(1).getCommand());
    }

    @Test
    void syncFromWithEmptyLogAppendsAll() {
        List<LogEntry> entries = Arrays.asList(
                new LogEntry(1, 1, "a"),
                new LogEntry(1, 2, "b")
        );
        log.syncFrom(0, entries);
        assertEquals(2, log.lastLogIndex());
    }

    @Test
    void syncFromTruncatesConflictingEntries() {
        log.append(1, "old-a");  // index 1, term 1
        log.append(1, "old-b");  // index 2, term 1

        // New leader has different entry at index 1
        List<LogEntry> newEntries = Arrays.asList(
                new LogEntry(2, 1, "new-a"),
                new LogEntry(2, 2, "new-b")
        );
        log.syncFrom(0, newEntries);
        assertEquals(2, log.lastLogIndex());
        assertEquals("new-a", log.get(1).getCommand());
        assertEquals(2, log.get(1).getTerm());
    }

    @Test
    void hasEntryAtReturnsCorrectResult() {
        log.append(1, "a");
        assertTrue(log.hasEntryAt(1, 1));
        assertFalse(log.hasEntryAt(1, 2));
        assertFalse(log.hasEntryAt(2, 1));
    }

    @Test
    void unappliedEntriesReturnedInOrder() {
        log.append(1, "a");
        log.append(1, "b");
        log.append(2, "c");
        log.setCommitIndex(2);

        List<LogEntry> unapplied = log.getUnappliedEntries();
        assertEquals(2, unapplied.size());
        assertEquals("a", unapplied.get(0).getCommand());
        assertEquals("b", unapplied.get(1).getCommand());

        log.setLastApplied(2);
        assertTrue(log.getUnappliedEntries().isEmpty());
    }
}
