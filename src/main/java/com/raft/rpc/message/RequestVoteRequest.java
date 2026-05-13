package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RequestVoteRequest extends RpcMessage {
    private final int term;
    private final String candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    @JsonCreator
    public RequestVoteRequest(
            @JsonProperty("term") int term,
            @JsonProperty("candidateId") String candidateId,
            @JsonProperty("lastLogIndex") int lastLogIndex,
            @JsonProperty("lastLogTerm") int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() { return term; }
    public String getCandidateId() { return candidateId; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }

    @Override
    public String toString() {
        return "RequestVoteRequest{term=" + term + ", candidateId='" + candidateId
                + "', lastLogIndex=" + lastLogIndex + ", lastLogTerm=" + lastLogTerm + "}";
    }
}
