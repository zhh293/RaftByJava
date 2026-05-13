package com.raft.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-memory Write-Ahead Log with 1-based indexing.
 * Index 0 is a virtual sentinel with term=0 (not stored).
 * Thread-confined to the Raft core thread.
 */
public class LogManager {
    private final List<LogEntry> entries = new ArrayList<>();

    private int commitIndex = 0;
    private int lastApplied = 0;

    /**
     * Append a new entry to the end of the log.
     * The entry's index will be set to lastLogIndex + 1,
     * overriding whatever index was passed in.
     */
    public LogEntry append(int term, String command) {
        int index = lastLogIndex() + 1;
        LogEntry entry = new LogEntry(term, index, command);
        entries.add(entry);
        return entry;
    }

    /**
     * Get a log entry by index. Returns the sentinel (term=0, index=0) for index 0.
     */
    public LogEntry get(int index) {
        if (index == 0) {
            return new LogEntry(0, 0, "");
        }
        if (index < 1 || index > entries.size()) {
            return null;
        }
        return entries.get(index - 1);
    }

    /**
     * Get the last log entry. Returns the sentinel if the log is empty.
     */
    public LogEntry getLast() {
        if (entries.isEmpty()) {
            return new LogEntry(0, 0, "");
        }
        return entries.get(entries.size() - 1);
    }

    /**
     * Get the index of the last log entry.
     */
    public int lastLogIndex() {
        return entries.size();
    }

    /**
     * Get the term of the last log entry.
     */
    public int lastLogTerm() {
        return getLast().getTerm();
    }

    /**
     * Check if an entry at the given index has the expected term.
     */
    public boolean hasEntryAt(int index, int term) {
        LogEntry entry = get(index);
        return entry != null && entry.getTerm() == term;
    }

    /**
     * Remove all entries from (and including) the given index.
     */
    public void truncateFrom(int index) {
        if (index <= entries.size()) {
            entries.subList(index - 1, entries.size()).clear();
        }
    }

    /**
     * Append entries from a leader. This truncates conflicting entries first,
     * then appends new ones. Duplicate entries (same index + term) are skipped.
     */
    public void syncFrom(int prevLogIndex, List<LogEntry> newEntries) {
        // If existing entry at prevLogIndex+1 conflicts, truncate from there
        int conflictIndex = prevLogIndex + 1;
        if (conflictIndex <= entries.size()) {
            LogEntry existing = get(conflictIndex);
            if (existing != null && existing.getTerm() != (newEntries.isEmpty() ? 0 : newEntries.get(0).getTerm())) {
                truncateFrom(conflictIndex);
            }
        }

        // Append new entries that are not already present
        int nextIndex = prevLogIndex + 1;
        for (LogEntry entry : newEntries) {
            if (nextIndex <= entries.size()) {
                // Entry already exists at this index; skip if it matches
                LogEntry existing = get(nextIndex);
                if (existing != null && existing.getTerm() == entry.getTerm()) {
                    nextIndex++;
                    continue;
                }
            }
            entries.add(new LogEntry(entry.getTerm(), nextIndex, entry.getCommand()));
            nextIndex++;
        }
    }

    /**
     * Get a slice of entries starting at the given index (inclusive) to end.
     */
    public List<LogEntry> getEntriesFrom(int startIndex) {
        if (startIndex < 1 || startIndex > entries.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(entries.subList(startIndex - 1, entries.size()));
    }

    // --- commit & apply ---

    public int getCommitIndex() { return commitIndex; }
    public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }

    public int getLastApplied() { return lastApplied; }
    public void setLastApplied(int lastApplied) { this.lastApplied = lastApplied; }

    /**
     * Return entries that are committed but not yet applied: (lastApplied, commitIndex].
     */
    public List<LogEntry> getUnappliedEntries() {
        if (lastApplied >= commitIndex) {
            return Collections.emptyList();
        }
        List<LogEntry> result = new ArrayList<>();
        for (int i = lastApplied + 1; i <= commitIndex; i++) {
            LogEntry e = get(i);
            if (e != null) {
                result.add(e);
            }
        }
        return result;
    }

    public int size() {
        return entries.size();
    }

    public List<LogEntry> getAllEntries() {
        return new ArrayList<>(entries);
    }
}
