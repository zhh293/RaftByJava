package com.raft.rpc.message;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AppendEntriesRequest.class, name = "APPEND_ENTRIES_REQUEST"),
        @JsonSubTypes.Type(value = AppendEntriesResponse.class, name = "APPEND_ENTRIES_RESPONSE"),
        @JsonSubTypes.Type(value = RequestVoteRequest.class, name = "REQUEST_VOTE_REQUEST"),
        @JsonSubTypes.Type(value = RequestVoteResponse.class, name = "REQUEST_VOTE_RESPONSE"),
        @JsonSubTypes.Type(value = PreVoteRequest.class, name = "PRE_VOTE_REQUEST"),
        @JsonSubTypes.Type(value = PreVoteResponse.class, name = "PRE_VOTE_RESPONSE"),
        @JsonSubTypes.Type(value = InstallSnapshotRequest.class, name = "INSTALL_SNAPSHOT_REQUEST"),
        @JsonSubTypes.Type(value = InstallSnapshotResponse.class, name = "INSTALL_SNAPSHOT_RESPONSE"),
        @JsonSubTypes.Type(value = ClientWriteRequest.class, name = "CLIENT_WRITE_REQUEST"),
        @JsonSubTypes.Type(value = ClientWriteResponse.class, name = "CLIENT_WRITE_RESPONSE"),
        @JsonSubTypes.Type(value = ClientReadRequest.class, name = "CLIENT_READ_REQUEST"),
        @JsonSubTypes.Type(value = ClientReadResponse.class, name = "CLIENT_READ_RESPONSE"),
        @JsonSubTypes.Type(value = MembershipChangeRequest.class, name = "MEMBERSHIP_CHANGE_REQUEST"),
        @JsonSubTypes.Type(value = MembershipChangeResponse.class, name = "MEMBERSHIP_CHANGE_RESPONSE"),
        @JsonSubTypes.Type(value = IdentificationMessage.class, name = "IDENTIFICATION")
})
public abstract class RpcMessage {
    // Marker base class for all Raft RPC messages
}
