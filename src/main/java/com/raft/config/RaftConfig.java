package com.raft.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RaftConfig {
    private final String nodeId;
    private final String listenHost;
    private final int listenPort;
    private final List<PeerConfig> peers;
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;

    @JsonCreator
    public RaftConfig(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("listenHost") String listenHost,
            @JsonProperty("listenPort") int listenPort,
            @JsonProperty("peers") List<PeerConfig> peers,
            @JsonProperty("electionTimeoutMinMs") int electionTimeoutMinMs,
            @JsonProperty("electionTimeoutMaxMs") int electionTimeoutMaxMs,
            @JsonProperty("heartbeatIntervalMs") int heartbeatIntervalMs) {
        this.nodeId = nodeId;
        this.listenHost = listenHost;
        this.listenPort = listenPort;
        this.peers = peers;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public String getNodeId() { return nodeId; }
    public String getListenHost() { return listenHost; }
    public int getListenPort() { return listenPort; }
    public List<PeerConfig> getPeers() { return peers; }
    public int getElectionTimeoutMinMs() { return electionTimeoutMinMs; }
    public int getElectionTimeoutMaxMs() { return electionTimeoutMaxMs; }
    public int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }

    /** Returns the PeerConfig for this node (self) from the peers list. */
    public PeerConfig getSelfPeer() {
        return peers.stream()
                .filter(p -> p.getNodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Self node " + nodeId + " not found in peers list"));
    }

    /** Returns all peers except this node. */
    public List<PeerConfig> getOtherPeers() {
        return peers.stream()
                .filter(p -> !p.getNodeId().equals(nodeId))
                .toList();
    }

    public int getClusterSize() {
        return peers.size();
    }

    /** Majority count: (clusterSize / 2) + 1 */
    public int getMajorityCount() {
        return (peers.size() / 2) + 1;
    }
}
