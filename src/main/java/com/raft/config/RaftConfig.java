package com.raft.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class RaftConfig {
    private final String nodeId;
    private final String listenHost;
    private final int listenPort;
    private List<PeerConfig> peers;
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;
    private final String dataDir;
    private final int snapshotThreshold;

    @JsonCreator
    public RaftConfig(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("listenHost") String listenHost,
            @JsonProperty("listenPort") int listenPort,
            @JsonProperty("peers") List<PeerConfig> peers,
            @JsonProperty("electionTimeoutMinMs") int electionTimeoutMinMs,
            @JsonProperty("electionTimeoutMaxMs") int electionTimeoutMaxMs,
            @JsonProperty("heartbeatIntervalMs") int heartbeatIntervalMs,
            @JsonProperty("dataDir") String dataDir,
            @JsonProperty("snapshotThreshold") int snapshotThreshold) {
        this.nodeId = nodeId;
        this.listenHost = listenHost;
        this.listenPort = listenPort;
        this.peers = peers;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.dataDir = dataDir != null ? dataDir : "data/" + nodeId;
        this.snapshotThreshold = snapshotThreshold > 0 ? snapshotThreshold : 1000;
    }

    public String getNodeId() { return nodeId; }
    public String getListenHost() { return listenHost; }
    public int getListenPort() { return listenPort; }
    public List<PeerConfig> getPeers() { return peers; }
    public int getElectionTimeoutMinMs() { return electionTimeoutMinMs; }
    public int getElectionTimeoutMaxMs() { return electionTimeoutMaxMs; }
    public int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public String getDataDir() { return dataDir; }
    public int getSnapshotThreshold() { return snapshotThreshold; }

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
                .collect(java.util.stream.Collectors.toList());
    }

    public int getClusterSize() {
        return peers.size();
    }

    /** Majority count: (clusterSize / 2) + 1 */
    public int getMajorityCount() {
        return (peers.size() / 2) + 1;
    }

    // --- Membership change support ---

    /**
     * Add a peer to the configuration. Used during membership changes.
     */
    public void addPeer(PeerConfig peer) {
        List<PeerConfig> newPeers = new ArrayList<>(peers);
        // Don't add duplicates
        boolean exists = newPeers.stream().anyMatch(p -> p.getNodeId().equals(peer.getNodeId()));
        if (!exists) {
            newPeers.add(peer);
            this.peers = newPeers;
        }
    }

    /**
     * Remove a peer from the configuration. Used during membership changes.
     */
    public void removePeer(String nodeId) {
        List<PeerConfig> newPeers = new ArrayList<>(peers);
        newPeers.removeIf(p -> p.getNodeId().equals(nodeId));
        this.peers = newPeers;
    }
}
