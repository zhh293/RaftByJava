package com.raft.core;

import com.raft.config.RaftConfig;
import com.raft.rpc.PeerConnectionManager;
import com.raft.rpc.message.AppendEntriesRequest;
import com.raft.rpc.message.AppendEntriesResponse;
import com.raft.rpc.message.InstallSnapshotRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Manages log replication from the leader to followers.
 * Thread-confined to the Raft core thread.
 */
public class ReplicationManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final NodeState state;
    private final LogManager logManager;
    private final PeerConnectionManager peerManager;
    private final String selfId;
    private int majorityCount;

    // Reference to SnapshotManager for InstallSnapshot fallback
    private SnapshotManager snapshotManager;

    public ReplicationManager(NodeState state, LogManager logManager,
                              PeerConnectionManager peerManager, RaftConfig config) {
        this.state = state;
        this.logManager = logManager;
        this.peerManager = peerManager;
        this.selfId = config.getNodeId();
        this.majorityCount = config.getMajorityCount();
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    public void setMajorityCount(int majorityCount) {
        this.majorityCount = majorityCount;
    }

    /**
     * Initialize nextIndex and matchIndex for all peers when becoming leader.
     */
    public void initialize() {
        int lastLogIndex = logManager.lastLogIndex();
        for (String peerId : peerManager.getPeerChannels().keySet()) {
            state.setNextIndex(peerId, lastLogIndex + 1);
            state.setMatchIndex(peerId, 0);
        }
    }

    /**
     * Replicate a new log entry to all peers.
     */
    public void replicateLog(LogEntry entry) {
        for (String peerId : peerManager.getPeerChannels().keySet()) {
            sendAppendEntries(peerId);
        }
    }

    /**
     * Send AppendEntries RPC to a specific peer (used for heartbeat too).
     */
    public void sendHeartbeatTo(String peerId) {
        sendAppendEntries(peerId);
    }

    /**
     * Send heartbeat (empty AppendEntries) to all peers.
     */
    public void sendHeartbeat() {
        for (String peerId : peerManager.getPeerChannels().keySet()) {
            sendAppendEntries(peerId);
        }
    }

    private void sendAppendEntries(String peerId) {
        int nextIdx = state.getNextIndex(peerId);
        int prevLogIndex = nextIdx - 1;

        // If the peer needs entries that have been compacted, send a snapshot instead
        if (prevLogIndex > 0 && prevLogIndex < logManager.getSnapshotLastIndex()) {
            sendInstallSnapshot(peerId);
            return;
        }

        LogEntry prevLog = logManager.get(prevLogIndex);
        int prevLogTerm = prevLog != null ? prevLog.getTerm() : 0;

        List<LogEntry> entries = logManager.getEntriesFrom(nextIdx);

        AppendEntriesRequest request = new AppendEntriesRequest(
                state.getCurrentTerm(),
                selfId,
                prevLogIndex,
                prevLogTerm,
                entries,
                logManager.getCommitIndex()
        );

        peerManager.sendToPeer(peerId, request);
    }

    /**
     * Send an InstallSnapshot RPC to a peer that is too far behind.
     */
    private void sendInstallSnapshot(String peerId) {
        if (snapshotManager == null) {
            log.warn("Cannot send InstallSnapshot: no SnapshotManager configured");
            return;
        }

        InstallSnapshotRequest request = new InstallSnapshotRequest(
                state.getCurrentTerm(),
                selfId,
                snapshotManager.getLastIncludedIndex(),
                snapshotManager.getLastIncludedTerm(),
                snapshotManager.getLastSnapshotData()
        );

        peerManager.sendToPeer(peerId, request);

        // After InstallSnapshot, advance nextIndex to just after the snapshot
        state.setNextIndex(peerId, snapshotManager.getLastIncludedIndex() + 1);
        state.setMatchIndex(peerId, snapshotManager.getLastIncludedIndex());

        log.info("Sent InstallSnapshot to peer {}: lastIncludedIndex={}",
                peerId, snapshotManager.getLastIncludedIndex());
    }

    /**
     * Handle a response from an AppendEntries RPC.
     */
    public void handleAppendResponse(String peerId, AppendEntriesResponse response) {
        if (!state.isLeader()) {
            return;
        }

        if (response.isSuccess()) {
            // Update matchIndex and nextIndex
            int sentUpTo = logManager.lastLogIndex();
            int newMatchIndex = Math.max(state.getMatchIndex(peerId), sentUpTo);
            state.setMatchIndex(peerId, newMatchIndex);
            state.setNextIndex(peerId, newMatchIndex + 1);
        } else {
            // Rejected: decrement nextIndex and retry
            int currentNext = state.getNextIndex(peerId);
            if (currentNext > 1) {
                state.setNextIndex(peerId, currentNext - 1);
                sendAppendEntries(peerId); // retry immediately
            }
        }
    }

    /**
     * Advance the commit index if a majority of peers have replicated an entry
     * from the current term.
     */
    public void advanceCommitIndex(String peerResponded) {
        if (!state.isLeader()) {
            return;
        }

        int leaderLastLogIndex = logManager.lastLogIndex();
        for (int n = leaderLastLogIndex; n > logManager.getCommitIndex(); n--) {
            LogEntry entry = logManager.get(n);
            if (entry == null || entry.getTerm() != state.getCurrentTerm()) {
                continue; // Only commit entries from the current term
            }

            int count = 1; // leader counts as 1
            for (String peerId : peerManager.getPeerChannels().keySet()) {
                if (state.getMatchIndex(peerId) >= n) {
                    count++;
                }
            }

            if (count >= majorityCount) {
                logManager.setCommitIndex(n);
                log.info("Advanced commitIndex to {} ({} replicas)", n, count);
                break;
            }
        }
    }
}
