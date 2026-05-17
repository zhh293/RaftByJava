package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Pre-Vote RPC response. Indicates whether the responder
 * would grant a vote to the candidate at the proposed nextTerm.
 */
public class PreVoteResponse extends RpcMessage {
    private final int term;
    private final boolean voteGranted;

    @JsonCreator
    public PreVoteResponse(
            @JsonProperty("term") int term,
            @JsonProperty("voteGranted") boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }

    @Override
    public String toString() {
        return "PreVoteResponse{term=" + term + ", voteGranted=" + voteGranted + "}";
    }
}
