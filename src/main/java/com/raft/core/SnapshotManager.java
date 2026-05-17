package com.raft.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages snapshots of the state machine for log compaction.
 * <p>
 * A snapshot captures the full state machine at a given log index,
 * allowing all log entries up to that index to be discarded.
 * <p>
 * Snapshot format on disk (snapshot.json):
 * <pre>
 * {
 *   "lastIncludedIndex": N,
 *   "lastIncludedTerm": T,
 *   "data": { "key1": "val1", ... }
 * }
 * </pre>
 * Thread-confined to the Raft core thread.
 */
public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path snapshotFile;

    // Configurable: trigger compaction when log exceeds this many entries
    private final int compactionThreshold;

    // Last snapshot metadata
    private int lastIncludedIndex = 0;
    private int lastIncludedTerm = 0;
    private Map<String, String> lastSnapshotData = new HashMap<>();
    private Map<String, ClientSessionTable.SessionEntry> lastSnapshotSessions = new HashMap<>();

    public SnapshotManager(Path dataDir, int compactionThreshold) {
        this.snapshotFile = dataDir.resolve("snapshot.json");
        this.compactionThreshold = compactionThreshold;
    }

    /**
     * Load existing snapshot from disk on startup.
     * Returns true if a snapshot was loaded.
     */
    public boolean loadSnapshot() {
        if (!Files.exists(snapshotFile)) {
            return false;
        }
        try {
            SnapshotData data = MAPPER.readValue(snapshotFile.toFile(), SnapshotData.class);
            this.lastIncludedIndex = data.lastIncludedIndex;
            this.lastIncludedTerm = data.lastIncludedTerm;
            this.lastSnapshotData = data.data != null ? data.data : new HashMap<>();
            this.lastSnapshotSessions = deserializeSessions(data.sessions);
            log.info("Loaded snapshot: lastIncludedIndex={}, lastIncludedTerm={}, keys={}, sessions={}",
                    lastIncludedIndex, lastIncludedTerm, lastSnapshotData.size(), lastSnapshotSessions.size());
            return true;
        } catch (IOException e) {
            log.error("Failed to load snapshot", e);
            return false;
        }
    }

    /**
     * Check if we should take a snapshot based on current log size.
     */
    public boolean shouldCompact(int logSize) {
        return logSize > compactionThreshold;
    }

    /**
     * Take a snapshot: save state machine and client session table to disk and compact the log.
     */
    public void takeSnapshot(StateMachine stateMachine, LogManager logManager,
                              ClientSessionTable sessionTable) {
        int snapshotIndex = logManager.getLastApplied();
        if (snapshotIndex <= lastIncludedIndex) {
            return; // nothing new to snapshot
        }

        LogEntry entry = logManager.get(snapshotIndex);
        if (entry == null) {
            return;
        }

        int snapshotTerm = entry.getTerm();
        Map<String, String> data = stateMachine.snapshot();
        Map<String, ClientSessionTable.SessionEntry> sessions =
                sessionTable != null ? sessionTable.snapshot() : new HashMap<>();

        // Write to disk
        SnapshotData sd = new SnapshotData();
        sd.lastIncludedIndex = snapshotIndex;
        sd.lastIncludedTerm = snapshotTerm;
        sd.data = data;
        sd.sessions = serializeSessions(sessions);

        try (FileOutputStream fos = new FileOutputStream(snapshotFile.toFile())) {
            MAPPER.writeValue(fos, sd);
            fos.getFD().sync();
        } catch (IOException e) {
            log.error("Failed to save snapshot", e);
            return;
        }

        this.lastIncludedIndex = snapshotIndex;
        this.lastIncludedTerm = snapshotTerm;
        this.lastSnapshotData = data;
        this.lastSnapshotSessions = sessions;

        // Compact the log
        logManager.applySnapshot(snapshotIndex, snapshotTerm);
        log.info("Snapshot taken at index={}, term={}, log compacted to {} entries",
                snapshotIndex, snapshotTerm, logManager.size());
    }

    /**
     * Install a snapshot received from the leader.
     */
    public void installSnapshot(int lastIndex, int lastTerm,
                                 Map<String, String> data,
                                 Map<String, ClientSessionTable.SessionEntry> sessions,
                                 StateMachine stateMachine,
                                 LogManager logManager,
                                 ClientSessionTable sessionTable) {
        // Save to disk
        SnapshotData sd = new SnapshotData();
        sd.lastIncludedIndex = lastIndex;
        sd.lastIncludedTerm = lastTerm;
        sd.data = data;
        sd.sessions = serializeSessions(sessions != null ? sessions : new HashMap<>());

        try (FileOutputStream fos = new FileOutputStream(snapshotFile.toFile())) {
            MAPPER.writeValue(fos, sd);
            fos.getFD().sync();
        } catch (IOException e) {
            log.error("Failed to save installed snapshot", e);
            return;
        }

        this.lastIncludedIndex = lastIndex;
        this.lastIncludedTerm = lastTerm;
        this.lastSnapshotData = data != null ? data : new HashMap<>();
        this.lastSnapshotSessions = sessions != null ? sessions : new HashMap<>();

        // Restore state machine from snapshot
        stateMachine.restoreFromSnapshot(data);

        // Restore client session table from snapshot
        if (sessionTable != null) {
            sessionTable.restoreFromSnapshot(this.lastSnapshotSessions);
        }

        // Compact log
        logManager.installSnapshot(lastIndex, lastTerm);

        log.info("Installed snapshot from leader: index={}, term={}, keys={}, sessions={}",
                lastIndex, lastTerm, lastSnapshotData.size(), lastSnapshotSessions.size());
    }

    public int getLastIncludedIndex() { return lastIncludedIndex; }
    public int getLastIncludedTerm() { return lastIncludedTerm; }
    public Map<String, String> getLastSnapshotData() { return lastSnapshotData; }
    public Map<String, ClientSessionTable.SessionEntry> getLastSnapshotSessions() { return lastSnapshotSessions; }

    /**
     * Internal POJO for snapshot serialization.
     */
    static class SnapshotData {
        public int lastIncludedIndex;
        public int lastIncludedTerm;
        public Map<String, String> data;
        public Map<String, Map<String, Object>> sessions;
    }

    // --- Session serialization helpers ---

    private Map<String, Map<String, Object>> serializeSessions(
            Map<String, ClientSessionTable.SessionEntry> sessions) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        for (Map.Entry<String, ClientSessionTable.SessionEntry> e : sessions.entrySet()) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("sequenceNumber", e.getValue().getSequenceNumber());
            entry.put("response", e.getValue().getResponse());
            result.put(e.getKey(), entry);
        }
        return result;
    }

    private Map<String, ClientSessionTable.SessionEntry> deserializeSessions(
            Map<String, Map<String, Object>> raw) {
        if (raw == null) return new HashMap<>();
        Map<String, ClientSessionTable.SessionEntry> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> e : raw.entrySet()) {
            Map<String, Object> vals = e.getValue();
            long seq = ((Number) vals.get("sequenceNumber")).longValue();
            String resp = vals.get("response") != null ? vals.get("response").toString() : null;
            result.put(e.getKey(), new ClientSessionTable.SessionEntry(seq, resp));
        }
        return result;
    }
}
