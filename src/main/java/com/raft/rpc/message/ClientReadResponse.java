package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientReadResponse extends RpcMessage {
    private final boolean success;
    private final String leaderHint;
    private final String key;
    private final String value;

    @JsonCreator
    public ClientReadResponse(
            @JsonProperty("success") boolean success,
            @JsonProperty("leaderHint") String leaderHint,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.success = success;
        this.leaderHint = leaderHint;
        this.key = key;
        this.value = value;
    }

    public boolean isSuccess() { return success; }
    public String getLeaderHint() { return leaderHint; }
    public String getKey() { return key; }
    public String getValue() { return value; }

    public static ClientReadResponse ok(String key, String value) {
        return new ClientReadResponse(true, null, key, value);
    }

    public static ClientReadResponse redirect(String leaderHint) {
        return new ClientReadResponse(false, leaderHint, null, null);
    }

    public static ClientReadResponse noLeader() {
        return new ClientReadResponse(false, null, null, "NO_LEADER: cluster is electing, please retry later");
    }

    @Override
    public String toString() {
        return "ClientReadResponse{success=" + success + ", key='" + key + "', value='" + value + "'}";
    }
}
