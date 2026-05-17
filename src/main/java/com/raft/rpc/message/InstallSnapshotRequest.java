package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * InstallSnapshot RPC request. Sent by the leader to followers
 * that are too far behind to catch up via AppendEntries (their
 * needed log entries have been compacted).
 * <p>
 * For simplicity, we send the entire snapshot in one message
 * (no chunking). The snapshot data is a serialized copy of the
 * state machine (key-value pairs).
 */
public class InstallSnapshotRequest extends RpcMessage {
    private final int term;
    private final String leaderId;
    private final int lastIncludedIndex;
    private final int lastIncludedTerm;
    private final Map<String, String> snapshotData;
    private final Map<String, Map<String, Object>> snapshotSessions;

    @JsonCreator
    public InstallSnapshotRequest(
            @JsonProperty("term") int term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("lastIncludedIndex") int lastIncludedIndex,
            @JsonProperty("lastIncludedTerm") int lastIncludedTerm,
            @JsonProperty("snapshotData") Map<String, String> snapshotData,
            @JsonProperty("snapshotSessions") Map<String, Map<String, Object>> snapshotSessions) {
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.snapshotData = snapshotData;
        this.snapshotSessions = snapshotSessions;
    }

    public int getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public int getLastIncludedIndex() { return lastIncludedIndex; }
    public int getLastIncludedTerm() { return lastIncludedTerm; }
    public Map<String, String> getSnapshotData() { return snapshotData; }
    public Map<String, Map<String, Object>> getSnapshotSessions() { return snapshotSessions; }

    @Override
    public String toString() {
        return "InstallSnapshotRequest{term=" + term + ", leaderId='" + leaderId
                + "', lastIncludedIndex=" + lastIncludedIndex
                + ", lastIncludedTerm=" + lastIncludedTerm
                + ", dataSize=" + (snapshotData != null ? snapshotData.size() : 0) + "}";
    }
}
