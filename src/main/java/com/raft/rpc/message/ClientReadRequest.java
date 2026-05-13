package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientReadRequest extends RpcMessage {
    private final String clientId;
    private final String key;

    @JsonCreator
    public ClientReadRequest(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("key") String key) {
        this.clientId = clientId;
        this.key = key;
    }

    public String getClientId() { return clientId; }
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "ClientReadRequest{clientId='" + clientId + "', key='" + key + "'}";
    }
}
