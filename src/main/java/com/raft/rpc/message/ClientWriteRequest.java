package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientWriteRequest extends RpcMessage {
    private final String clientId;
    private final long sequenceNumber;
    private final String command;
    /** Non-null when this request has been forwarded by a follower to the leader. */
    private final String forwardingId;

    @JsonCreator
    public ClientWriteRequest(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("command") String command,
            @JsonProperty("forwardingId") String forwardingId) {
        this.clientId = clientId;
        this.sequenceNumber = sequenceNumber;
        this.command = command;
        this.forwardingId = forwardingId;
    }

    /** Convenience constructor for direct client requests (no forwarding). */
    public ClientWriteRequest(String clientId, long sequenceNumber, String command) {
        this(clientId, sequenceNumber, command, null);
    }

    public String getClientId() { return clientId; }
    public long getSequenceNumber() { return sequenceNumber; }
    public String getCommand() { return command; }
    public String getForwardingId() { return forwardingId; }
    public boolean isForwarded() { return forwardingId != null; }

    @Override
    public String toString() {
        return "ClientWriteRequest{clientId='" + clientId + "', seq=" + sequenceNumber
                + ", command='" + command + "'" + (forwardingId != null ? ", fwd=" + forwardingId : "") + "}";
    }
}
