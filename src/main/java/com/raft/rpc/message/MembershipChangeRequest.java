package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Client request to add or remove a node from the cluster.
 * The leader processes this by appending a special configuration
 * log entry. Uses the single-node change approach (one change at a time).
 */
public class MembershipChangeRequest extends RpcMessage {

    public enum ChangeType {
        ADD_NODE,
        REMOVE_NODE
    }

    private final ChangeType changeType;
    private final String targetNodeId;
    private final String targetHost;
    private final int targetPort;

    @JsonCreator
    public MembershipChangeRequest(
            @JsonProperty("changeType") ChangeType changeType,
            @JsonProperty("targetNodeId") String targetNodeId,
            @JsonProperty("targetHost") String targetHost,
            @JsonProperty("targetPort") int targetPort) {
        this.changeType = changeType;
        this.targetNodeId = targetNodeId;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
    }

    public ChangeType getChangeType() { return changeType; }
    public String getTargetNodeId() { return targetNodeId; }
    public String getTargetHost() { return targetHost; }
    public int getTargetPort() { return targetPort; }

    @Override
    public String toString() {
        return "MembershipChangeRequest{changeType=" + changeType
                + ", targetNodeId='" + targetNodeId + "'"
                + ", targetHost='" + targetHost + "'"
                + ", targetPort=" + targetPort + "}";
    }
}
