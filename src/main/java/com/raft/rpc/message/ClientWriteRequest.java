package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientWriteRequest extends RpcMessage {
    private final String clientId;
    private final long sequenceNumber;
    private final String command;

    @JsonCreator
    public ClientWriteRequest(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("command") String command) {
        this.clientId = clientId;
        this.sequenceNumber = sequenceNumber;
        this.command = command;
    }

    public String getClientId() { return clientId; }
    public long getSequenceNumber() { return sequenceNumber; }
    public String getCommand() { return command; }

    @Override
    public String toString() {
        return "ClientWriteRequest{clientId='" + clientId + "', seq=" + sequenceNumber
                + ", command='" + command + "'}";
    }
}
