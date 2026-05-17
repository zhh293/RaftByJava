package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientReadRequest extends RpcMessage {
    private final String clientId;
    private final String key;
    /**
     * If true, the client requires linearizable consistency (read via Leader ReadIndex).
     * If false, allows eventual-consistency reads from any node.
     */
    private final boolean linearizable;
    /**
     * Minimum appliedIndex the client requires for eventual-consistency reads.
     * A value <= 0 means the client has no freshness requirement.
     */
    private final int minAppliedIndex;

    @JsonCreator
    public ClientReadRequest(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("key") String key,
            @JsonProperty("linearizable") boolean linearizable,
            @JsonProperty("minAppliedIndex") int minAppliedIndex) {
        this.clientId = clientId;
        this.key = key;
        this.linearizable = linearizable;
        this.minAppliedIndex = minAppliedIndex;
    }

    /** Convenience: linearizable read (default). */
    public ClientReadRequest(String clientId, String key) {
        this(clientId, key, true, 0);
    }

    public String getClientId() { return clientId; }
    public String getKey() { return key; }
    public boolean isLinearizable() { return linearizable; }
    public int getMinAppliedIndex() { return minAppliedIndex; }

    @Override
    public String toString() {
        return "ClientReadRequest{clientId='" + clientId + "', key='" + key
                + "', linearizable=" + linearizable + ", minApplied=" + minAppliedIndex + "}";
    }
}
