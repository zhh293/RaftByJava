package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Pre-Vote RPC request. Sent before starting a real election
 * to check if the candidate would receive votes without
 * incrementing the term. Prevents term inflation from
 * partitioned nodes.
 */
public class PreVoteRequest extends RpcMessage {
    private final int nextTerm;       // the term the candidate would use (currentTerm + 1)
    private final String candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    @JsonCreator
    public PreVoteRequest(
            @JsonProperty("nextTerm") int nextTerm,
            @JsonProperty("candidateId") String candidateId,
            @JsonProperty("lastLogIndex") int lastLogIndex,
            @JsonProperty("lastLogTerm") int lastLogTerm) {
        this.nextTerm = nextTerm;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getNextTerm() { return nextTerm; }
    public String getCandidateId() { return candidateId; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }

    @Override
    public String toString() {
        return "PreVoteRequest{nextTerm=" + nextTerm + ", candidateId='" + candidateId
                + "', lastLogIndex=" + lastLogIndex + ", lastLogTerm=" + lastLogTerm + "}";
    }
}
