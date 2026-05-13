package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientWriteResponse extends RpcMessage {
    private final boolean success;
    private final String leaderHint;
    private final String result;

    @JsonCreator
    public ClientWriteResponse(
            @JsonProperty("success") boolean success,
            @JsonProperty("leaderHint") String leaderHint,
            @JsonProperty("result") String result) {
        this.success = success;
        this.leaderHint = leaderHint;
        this.result = result;
    }

    public boolean isSuccess() { return success; }
    public String getLeaderHint() { return leaderHint; }
    public String getResult() { return result; }

    public static ClientWriteResponse ok(String result) {
        return new ClientWriteResponse(true, null, result);
    }

    public static ClientWriteResponse redirect(String leaderHint) {
        return new ClientWriteResponse(false, leaderHint, null);
    }

    @Override
    public String toString() {
        return "ClientWriteResponse{success=" + success + ", leaderHint='" + leaderHint + "', result='" + result + "'}";
    }
}
