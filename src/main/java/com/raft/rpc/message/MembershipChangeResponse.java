package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response to a membership change request.
 */
public class MembershipChangeResponse extends RpcMessage {
    private final boolean success;
    private final String leaderHint;
    private final String result;

    @JsonCreator
    public MembershipChangeResponse(
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

    public static MembershipChangeResponse ok(String result) {
        return new MembershipChangeResponse(true, null, result);
    }

    public static MembershipChangeResponse redirect(String leaderHint) {
        return new MembershipChangeResponse(false, leaderHint, null);
    }

    public static MembershipChangeResponse fail(String reason) {
        return new MembershipChangeResponse(false, null, reason);
    }

    @Override
    public String toString() {
        return "MembershipChangeResponse{success=" + success + ", result='" + result + "'}";
    }
}
