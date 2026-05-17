package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientWriteResponse extends RpcMessage {
    private final boolean success;
    private final String leaderHint;
    private final String result;
    /** Non-null when this is a response to a forwarded write request. */
    private final String forwardingId;

    @JsonCreator
    public ClientWriteResponse(
            @JsonProperty("success") boolean success,
            @JsonProperty("leaderHint") String leaderHint,
            @JsonProperty("result") String result,
            @JsonProperty("forwardingId") String forwardingId) {
        this.success = success;
        this.leaderHint = leaderHint;
        this.result = result;
        this.forwardingId = forwardingId;
    }

    public boolean isSuccess() { return success; }
    public String getLeaderHint() { return leaderHint; }
    public String getResult() { return result; }
    public String getForwardingId() { return forwardingId; }

    public static ClientWriteResponse ok(String result) {
        return new ClientWriteResponse(true, null, result, null);
    }

    public static ClientWriteResponse okForwarded(String result, String forwardingId) {
        return new ClientWriteResponse(true, null, result, forwardingId);
    }

    public static ClientWriteResponse redirect(String leaderHint) {
        return new ClientWriteResponse(false, leaderHint, null, null);
    }

    public static ClientWriteResponse noLeader() {
        return new ClientWriteResponse(false, null, "NO_LEADER: cluster is electing, please retry later", null);
    }

    public static ClientWriteResponse failForwarded(String reason, String forwardingId) {
        return new ClientWriteResponse(false, null, reason, forwardingId);
    }

    @Override
    public String toString() {
        return "ClientWriteResponse{success=" + success + ", leaderHint='" + leaderHint + "', result='" + result + "'}";
    }
}
