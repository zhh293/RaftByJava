package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * InstallSnapshot RPC response.
 */
public class InstallSnapshotResponse extends RpcMessage {
    private final int term;

    @JsonCreator
    public InstallSnapshotResponse(@JsonProperty("term") int term) {
        this.term = term;
    }

    public int getTerm() { return term; }

    @Override
    public String toString() {
        return "InstallSnapshotResponse{term=" + term + "}";
    }
}
