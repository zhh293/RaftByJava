package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AppendEntriesResponse extends RpcMessage {
    private final int term;
    private final boolean success;
    private final String nodeId;

    @JsonCreator
    public AppendEntriesResponse(
            @JsonProperty("term") int term,
            @JsonProperty("success") boolean success,
            @JsonProperty("nodeId") String nodeId) {
        this.term = term;
        this.success = success;
        this.nodeId = nodeId;
    }

    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public String getNodeId() { return nodeId; }

    @Override
    public String toString() {
        return "AppendEntriesResponse{term=" + term + ", success=" + success + ", nodeId='" + nodeId + "'}";
    }
}
