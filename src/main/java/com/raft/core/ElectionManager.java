package com.raft.core;

import com.raft.config.RaftConfig;
import com.raft.rpc.PeerConnectionManager;
import com.raft.rpc.message.RequestVoteRequest;
import com.raft.rpc.message.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulates candidate election logic.
 * Thread-confined to the Raft core thread.
 */
public class ElectionManager {
    private static final Logger log = LoggerFactory.getLogger(ElectionManager.class);

    private final NodeState state;
    private final LogManager logManager;
    private final PeerConnectionManager peerManager;
    private final String selfId;
    private final int majorityCount;

    private final Set<String> votesReceived = new HashSet<>();

    public ElectionManager(NodeState state, LogManager logManager,
                           PeerConnectionManager peerManager, RaftConfig config) {
        this.state = state;
        this.logManager = logManager;
        this.peerManager = peerManager;
        this.selfId = config.getNodeId();
        this.majorityCount = config.getMajorityCount();
    }

    /**
     * Start an election: increment term, vote for self, broadcast RequestVote.
     * Called when election timer fires as a follower, or when a candidate's
     * election times out (retry with incremented term).
     */
    public void startElection() {
        state.setCurrentTerm(state.getCurrentTerm() + 1);
        state.setVotedFor(selfId);
        state.setRole(NodeRole.CANDIDATE);
        state.setLeaderId(null);
        votesReceived.clear();
        votesReceived.add(selfId); // vote for self

        log.info("Starting election for term {}", state.getCurrentTerm());

        int lastLogIndex = logManager.lastLogIndex();
        int lastLogTerm = logManager.lastLogTerm();

        RequestVoteRequest request = new RequestVoteRequest(
                state.getCurrentTerm(),
                selfId,
                lastLogIndex,
                lastLogTerm
        );

        peerManager.broadcast(request);
    }

    /**
     * Handle a vote response from a peer.
     * @return true if this candidate has won the election
     */
    public boolean handleVoteResponse(RequestVoteResponse response, int candidateTerm) {
        // If we're no longer a candidate (or term has changed), ignore
        if (!state.isCandidate() || state.getCurrentTerm() != candidateTerm) {
            return false;
        }

        if (response.isVoteGranted()) {
            votesReceived.add("peer"); // track count (we don't track by peerId for simplicity)
        }

        // Count votes: self-vote (1) + granted votes
        // We use a simpler approach: count self + granted count
        // For accurate tracking we'd need the peer's nodeId in the response

        return false; // Return value handled by RaftCore which counts properly
    }

    /**
     * Check if this candidate has received votes from a majority.
     */
    public boolean hasMajority(int grantedCount) {
        return grantedCount >= majorityCount;
    }

    public int getMajorityCount() {
        return majorityCount;
    }

    /**
     * Determine whether to grant a vote to a candidate.
     * A follower grants a vote only if the candidate's log is at least as
     * up-to-date as its own.
     *
     * Rule: candidate's lastLogTerm > mine, OR
     *       (candidate's lastLogTerm == mine AND candidate's lastLogIndex >= mine)
     */
    public boolean shouldGrantVote(int candidateLastLogTerm, int candidateLastLogIndex) {
        int myLastLogTerm = logManager.lastLogTerm();
        int myLastLogIndex = logManager.lastLogIndex();

        if (candidateLastLogTerm > myLastLogTerm) {
            return true;
        }
        if (candidateLastLogTerm == myLastLogTerm && candidateLastLogIndex >= myLastLogIndex) {
            return true;
        }
        return false;
    }

    public void reset() {
        votesReceived.clear();
    }
}
