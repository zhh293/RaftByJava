package com.raft.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Thread-confined mutable Raft state. All access must be from the Raft core thread.
 */
public class NodeState {
    private final String nodeId;

    // Persistent state (written to disk via PersistenceManager)
    private int currentTerm = 0;
    private String votedFor = null;

    // Optional persistence callback
    private PersistenceManager persistenceManager;

    // Role
    private volatile NodeRole role = NodeRole.FOLLOWER;
    private String leaderId = null;

    // Leader-only: index of next log entry to send to each peer (initialized to leader's last log index + 1)
    private final Map<String, Integer> nextIndex = new HashMap<>();

    // Leader-only: highest log entry known to be replicated on each peer
    private final Map<String, Integer> matchIndex = new HashMap<>();

    public NodeState(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Set the persistence manager. When set, every setCurrentTerm/setVotedFor
     * call will automatically flush state to disk.
     */
    public void setPersistenceManager(PersistenceManager pm) {
        this.persistenceManager = pm;
    }

    // --- term ---
    public int getCurrentTerm() { return currentTerm; }
    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
        persistMeta();
    }

    // --- votedFor ---
    public String getVotedFor() { return votedFor; }
    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        persistMeta();
    }

    private void persistMeta() {
        if (persistenceManager != null) {
            persistenceManager.saveMeta(currentTerm, votedFor);
        }
    }

    // --- role ---
    public NodeRole getRole() { return role; }
    public void setRole(NodeRole role) { this.role = role; }
    public boolean isLeader() { return role == NodeRole.LEADER; }
    public boolean isFollower() { return role == NodeRole.FOLLOWER; }
    public boolean isCandidate() { return role == NodeRole.CANDIDATE; }

    // --- leaderId ---
    public String getLeaderId() { return leaderId; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }

    // --- nextIndex (leader only) ---
    public Map<String, Integer> getNextIndex() { return nextIndex; }
    public void setNextIndex(String peerId, int index) { nextIndex.put(peerId, index); }
    public int getNextIndex(String peerId) { return nextIndex.getOrDefault(peerId, 1); }

    // --- matchIndex (leader only) ---
    public Map<String, Integer> getMatchIndex() { return matchIndex; }
    public void setMatchIndex(String peerId, int index) { matchIndex.put(peerId, index); }
    public int getMatchIndex(String peerId) { return matchIndex.getOrDefault(peerId, 0); }

    // Convenience
    public String getNodeId() { return nodeId; }

    /** Reset election-related state (called when stepping down to follower). */
    public void resetForNewTerm() {
        votedFor = null;
        leaderId = null;
    }

    /** Clear leader-only state. */
    public void clearLeaderState() {
        nextIndex.clear();
        matchIndex.clear();
    }

    @Override
    public String toString() {
        return "NodeState{nodeId='" + nodeId + "', role=" + role + ", term=" + currentTerm + "}";
    }
}
