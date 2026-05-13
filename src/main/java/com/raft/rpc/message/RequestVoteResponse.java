package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RequestVoteResponse extends RpcMessage {
    private final int term;
    private final boolean voteGranted;

    @JsonCreator
    public RequestVoteResponse(
            @JsonProperty("term") int term,
            @JsonProperty("voteGranted") boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }

    @Override
    public String toString() {
        return "RequestVoteResponse{term=" + term + ", voteGranted=" + voteGranted + "}";
    }
}
