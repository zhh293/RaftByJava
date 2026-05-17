package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientReadResponse extends RpcMessage {
    private final boolean success;
    private final String leaderHint;
    private final String key;
    private final String value;
    /** The appliedIndex at the time of read, so clients can track freshness. */
    private final int appliedIndex;

    @JsonCreator
    public ClientReadResponse(
            @JsonProperty("success") boolean success,
            @JsonProperty("leaderHint") String leaderHint,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("appliedIndex") int appliedIndex) {
        this.success = success;
        this.leaderHint = leaderHint;
        this.key = key;
        this.value = value;
        this.appliedIndex = appliedIndex;
    }

    public boolean isSuccess() { return success; }
    public String getLeaderHint() { return leaderHint; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    public int getAppliedIndex() { return appliedIndex; }

    public static ClientReadResponse ok(String key, String value, int appliedIndex) {
        return new ClientReadResponse(true, null, key, value, appliedIndex);
    }

    /** Backward-compatible convenience (appliedIndex = 0). */
    public static ClientReadResponse ok(String key, String value) {
        return ok(key, value, 0);
    }

    public static ClientReadResponse redirect(String leaderHint) {
        return new ClientReadResponse(false, leaderHint, null, null, 0);
    }

    public static ClientReadResponse noLeader() {
        return new ClientReadResponse(false, null, null,
                "NO_LEADER: cluster is electing, please retry later", 0);
    }

    public static ClientReadResponse stale(String key, int appliedIndex, int minRequired) {
        return new ClientReadResponse(false, null, key,
                "STALE: appliedIndex=" + appliedIndex + " < minRequired=" + minRequired, appliedIndex);
    }

    @Override
    public String toString() {
        return "ClientReadResponse{success=" + success + ", key='" + key
                + "', value='" + value + "', appliedIndex=" + appliedIndex + "}";
    }
}
