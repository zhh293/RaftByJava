package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IdentificationMessage extends RpcMessage {
    private final String nodeId;

    @JsonCreator
    public IdentificationMessage(@JsonProperty("nodeId") String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() { return nodeId; }

    @Override
    public String toString() {
        return "IdentificationMessage{nodeId='" + nodeId + "'}";
    }
}
