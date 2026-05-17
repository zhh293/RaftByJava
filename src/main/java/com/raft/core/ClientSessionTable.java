package com.raft.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Client session tracking for idempotency.
 * <p>
 * Each client is identified by a clientId and tracks the latest sequenceNumber
 * that has been applied. If a duplicate request arrives (same clientId + sequenceNumber
 * that was already applied), the cached response is returned instead of re-executing.
 * <p>
 * Thread-confined to the Raft core thread.
 */
public class ClientSessionTable {
    private static final Logger log = LoggerFactory.getLogger(ClientSessionTable.class);

    /**
     * Maps clientId -> the latest applied session entry.
     */
    private final Map<String, SessionEntry> sessions = new HashMap<>();

    /**
     * Check if this request is a duplicate of one already applied.
     */
    public boolean isDuplicate(String clientId, long sequenceNumber) {
        SessionEntry entry = sessions.get(clientId);
        return entry != null && entry.sequenceNumber >= sequenceNumber;
    }

    /**
     * Get the cached response for a duplicate request.
     */
    public String getCachedResponse(String clientId) {
        SessionEntry entry = sessions.get(clientId);
        return entry != null ? entry.response : null;
    }

    /**
     * Record a newly applied command for a client.
     */
    public void recordApplied(String clientId, long sequenceNumber, String response) {
        sessions.put(clientId, new SessionEntry(sequenceNumber, response));
        log.debug("Recorded session: clientId={}, seq={}", clientId, sequenceNumber);
    }

    /**
     * Get a snapshot of all sessions (for inclusion in snapshots).
     */
    public Map<String, SessionEntry> snapshot() {
        return new HashMap<>(sessions);
    }

    /**
     * Restore sessions from a snapshot.
     */
    public void restoreFromSnapshot(Map<String, SessionEntry> data) {
        sessions.clear();
        if (data != null) {
            sessions.putAll(data);
        }
    }

    public int size() {
        return sessions.size();
    }

    /**
     * Tracks the latest applied sequence number and response for a client.
     */
    public static class SessionEntry {
        private final long sequenceNumber;
        private final String response;

        public SessionEntry(long sequenceNumber, String response) {
            this.sequenceNumber = sequenceNumber;
            this.response = response;
        }

        public long getSequenceNumber() { return sequenceNumber; }
        public String getResponse() { return response; }
    }
}
