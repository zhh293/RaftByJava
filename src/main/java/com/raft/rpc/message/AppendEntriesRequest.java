package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.raft.core.LogEntry;

import java.util.Collections;
import java.util.List;

public class AppendEntriesRequest extends RpcMessage {
    private final int term;
    private final String leaderId;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final List<LogEntry> entries;
    private final int leaderCommit;

    @JsonCreator
    public AppendEntriesRequest(
            @JsonProperty("term") int term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("prevLogIndex") int prevLogIndex,
            @JsonProperty("prevLogTerm") int prevLogTerm,
            @JsonProperty("entries") List<LogEntry> entries,
            @JsonProperty("leaderCommit") int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? entries : Collections.emptyList();
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public int getPrevLogIndex() { return prevLogIndex; }
    public int getPrevLogTerm() { return prevLogTerm; }
    public List<LogEntry> getEntries() { return entries; }
    public int getLeaderCommit() { return leaderCommit; }

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }

    @Override
    public String toString() {
        return "AppendEntriesRequest{term=" + term + ", leaderId='" + leaderId
                + "', prevLogIndex=" + prevLogIndex + ", prevLogTerm=" + prevLogTerm
                + ", entries=" + entries.size() + ", leaderCommit=" + leaderCommit + "}";
    }
}
