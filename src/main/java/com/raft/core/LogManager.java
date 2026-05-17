package com.raft.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Write-Ahead Log with 1-based indexing.
 * Index 0 is a virtual sentinel with term=0 (not stored).
 * Thread-confined to the Raft core thread.
 * <p>
 * When a PersistenceManager is attached, all mutations are automatically
 * persisted to disk.
 */
public class LogManager {
    private final List<LogEntry> entries = new ArrayList<>();

    private int commitIndex = 0;
    private int lastApplied = 0;

    // Snapshot support: entries before snapshotLastIndex are trimmed.
    // The sentinel is replaced by the snapshot's last entry metadata.
    private int snapshotLastIndex = 0;
    private int snapshotLastTerm = 0;

    // Optional persistence
    private PersistenceManager persistenceManager;

    public void setPersistenceManager(PersistenceManager pm) {
        this.persistenceManager = pm;
    }

    /**
     * Load entries from disk on startup. Must be called before any other operations.
     */
    public void loadFromDisk() {
        if (persistenceManager == null) return;
        List<LogEntry> loaded = persistenceManager.loadEntries();
        entries.clear();
        entries.addAll(loaded);
    }

    /**
     * Append a new entry to the end of the log.
     * The entry's index will be set to lastLogIndex + 1.
     */
    public LogEntry append(int term, String command) {
        int index = lastLogIndex() + 1;
        LogEntry entry = new LogEntry(term, index, command);
        entries.add(entry);
        if (persistenceManager != null) {
            persistenceManager.appendEntry(entry);
        }
        return entry;
    }

    /**
     * Get a log entry by index. Returns the sentinel (term=0, index=0) for index 0,
     * or the snapshot sentinel if a snapshot has been applied.
     */
    public LogEntry get(int index) {
        if (index == 0) {
            return new LogEntry(0, 0, "");
        }
        // If index matches the snapshot boundary, return virtual sentinel
        if (index == snapshotLastIndex && snapshotLastIndex > 0) {
            return new LogEntry(snapshotLastTerm, snapshotLastIndex, "");
        }
        // Index is in the compacted region
        if (index < snapshotLastIndex) {
            return null;
        }
        int arrayIndex = index - snapshotLastIndex - 1;
        if (arrayIndex < 0 || arrayIndex >= entries.size()) {
            return null;
        }
        return entries.get(arrayIndex);
    }

    /**
     * Get the last log entry. Returns the sentinel if the log is empty.
     */
    public LogEntry getLast() {
        if (entries.isEmpty()) {
            if (snapshotLastIndex > 0) {
                return new LogEntry(snapshotLastTerm, snapshotLastIndex, "");
            }
            return new LogEntry(0, 0, "");
        }
        return entries.get(entries.size() - 1);
    }

    /**
     * Get the index of the last log entry.
     */
    public int lastLogIndex() {
        if (entries.isEmpty()) {
            return snapshotLastIndex;
        }
        return entries.get(entries.size() - 1).getIndex();
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
        int arrayStart = index - snapshotLastIndex - 1;
        if (arrayStart < 0) arrayStart = 0;
        if (arrayStart < entries.size()) {
            entries.subList(arrayStart, entries.size()).clear();
        }
        if (persistenceManager != null) {
            persistenceManager.truncateFrom(index);
        }
    }

    /**
     * Append entries from a leader. This truncates conflicting entries first,
     * then appends new ones. Duplicate entries (same index + term) are skipped.
     */
    public void syncFrom(int prevLogIndex, List<LogEntry> newEntries) {
        int nextIndex = prevLogIndex + 1;
        for (LogEntry entry : newEntries) {
            LogEntry existing = get(nextIndex);
            if (existing != null) {
                if (existing.getTerm() != entry.getTerm()) {
                    truncateFrom(nextIndex);
                    appendRaw(new LogEntry(entry.getTerm(), nextIndex, entry.getCommand()));
                }
                // else same term, skip
            } else {
                appendRaw(new LogEntry(entry.getTerm(), nextIndex, entry.getCommand()));
            }
            nextIndex++;
        }
    }

    /**
     * Low-level append (used by syncFrom). Adds to in-memory list and persists.
     */
    private void appendRaw(LogEntry entry) {
        entries.add(entry);
        if (persistenceManager != null) {
            persistenceManager.appendEntry(entry);
        }
    }

    /**
     * Get a slice of entries starting at the given index (inclusive) to end.
     */
    public List<LogEntry> getEntriesFrom(int startIndex) {
        if (startIndex > lastLogIndex()) {
            return Collections.emptyList();
        }
        int arrayStart = startIndex - snapshotLastIndex - 1;
        if (arrayStart < 0) arrayStart = 0;
        if (arrayStart >= entries.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(entries.subList(arrayStart, entries.size()));
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

    // --- Snapshot support ---

    public int getSnapshotLastIndex() { return snapshotLastIndex; }
    public int getSnapshotLastTerm() { return snapshotLastTerm; }

    /**
     * Apply a snapshot: discard all entries up to and including lastIncludedIndex,
     * set the snapshot sentinel.
     */
    public void applySnapshot(int lastIncludedIndex, int lastIncludedTerm) {
        // Remove all entries up to lastIncludedIndex
        List<LogEntry> remaining = new ArrayList<>();
        for (LogEntry e : entries) {
            if (e.getIndex() > lastIncludedIndex) {
                remaining.add(e);
            }
        }
        entries.clear();
        entries.addAll(remaining);

        this.snapshotLastIndex = lastIncludedIndex;
        this.snapshotLastTerm = lastIncludedTerm;

        // Advance commitIndex and lastApplied if needed
        if (commitIndex < lastIncludedIndex) {
            commitIndex = lastIncludedIndex;
        }
        if (lastApplied < lastIncludedIndex) {
            lastApplied = lastIncludedIndex;
        }

        // Rewrite WAL to only contain remaining entries
        if (persistenceManager != null) {
            persistenceManager.rewriteWal(remaining);
        }
    }

    /**
     * Install a snapshot received from leader: discard entire log,
     * set snapshot sentinel.
     */
    public void installSnapshot(int lastIncludedIndex, int lastIncludedTerm) {
        entries.clear();
        this.snapshotLastIndex = lastIncludedIndex;
        this.snapshotLastTerm = lastIncludedTerm;
        this.commitIndex = lastIncludedIndex;
        this.lastApplied = lastIncludedIndex;

        if (persistenceManager != null) {
            persistenceManager.rewriteWal(Collections.emptyList());
        }
    }
}
