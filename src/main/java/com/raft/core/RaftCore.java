package com.raft.core;

import com.raft.config.RaftConfig;
import com.raft.rpc.PeerConnectionManager;
import com.raft.rpc.RaftMessageHandler.RaftCoreDelegate;
import com.raft.rpc.message.*;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * The heart of the Raft implementation. All Raft algorithm logic lives here,
 * running on a single dedicated thread (the raft executor).
 */
public class RaftCore implements RaftCoreDelegate {
    private static final Logger log = LoggerFactory.getLogger(RaftCore.class);

    private final NodeState state;
    private final LogManager logManager;
    private final StateMachine stateMachine;
    private final TimerManager timerManager;
    private final ElectionManager electionManager;
    private final ReplicationManager replicationManager;
    private final PeerConnectionManager peerManager;
    private final String selfId;
    private final int majorityCount;

    // --- candidate state ---
    private final Set<String> voteGrants = new HashSet<>();
    private int candidateTerm = -1;

    public RaftCore(RaftConfig config,
                    NodeState state,
                    LogManager logManager,
                    StateMachine stateMachine,
                    TimerManager timerManager,
                    PeerConnectionManager peerManager) {
        this.state = state;
        this.logManager = logManager;
        this.stateMachine = stateMachine;
        this.timerManager = timerManager;
        this.peerManager = peerManager;
        this.selfId = config.getNodeId();
        this.majorityCount = config.getMajorityCount();

        this.electionManager = new ElectionManager(state, logManager, peerManager, config);
        this.replicationManager = new ReplicationManager(state, logManager, peerManager, config);

        // Wire up timer callbacks
        timerManager.setElectionTimeoutCallback(this::onElectionTimeout);
        timerManager.setHeartbeatCallback(this::sendHeartbeat);
    }

    // ================================================================
    // Initialization
    // ================================================================

    public void initialize() {
        log.info("RaftCore initializing as FOLLOWER");
        state.setRole(NodeRole.FOLLOWER);
        state.setCurrentTerm(0);
        state.setVotedFor(null);
        timerManager.resetElectionTimer();
    }

    // ================================================================
    // Election timeout
    // ================================================================

    private void onElectionTimeout() {
        if (state.isLeader()) {
            return; // leaders don't time out for election
        }
        log.info("Election timeout fired. Current role: {}, term: {}",
                state.getRole(), state.getCurrentTerm());
        electionManager.startElection();
        candidateTerm = state.getCurrentTerm();
        voteGrants.clear();
        voteGrants.add(selfId);
        timerManager.resetElectionTimer(); // schedule re-election if this one fails
    }

    // ================================================================
    // AppendEntries
    // ================================================================

    @Override
    public void onAppendEntriesRequest(AppendEntriesRequest req, Channel channel) {
        log.debug("Received AppendEntries: term={}, leaderId={}, prevLogIndex={}, entries={}",
                req.getTerm(), req.getLeaderId(), req.getPrevLogIndex(), req.getEntries().size());

        // If leader's term is older than ours, reject
        if (req.getTerm() < state.getCurrentTerm()) {
            send(channel, new AppendEntriesResponse(state.getCurrentTerm(), false, selfId));
            return;
        }

        // Valid leader with >= our term: accept authority
        stepDown(req.getTerm());
        state.setLeaderId(req.getLeaderId());
        timerManager.resetElectionTimer();

        // Validate prevLogIndex/prevLogTerm
        if (!logManager.hasEntryAt(req.getPrevLogIndex(), req.getPrevLogTerm())) {
            send(channel, new AppendEntriesResponse(state.getCurrentTerm(), false, selfId));
            return;
        }

        // Sync log entries
        logManager.syncFrom(req.getPrevLogIndex(), req.getEntries());

        // Update commit index
        if (req.getLeaderCommit() > logManager.getCommitIndex()) {
            int newCommit = Math.min(req.getLeaderCommit(), logManager.lastLogIndex());
            logManager.setCommitIndex(newCommit);
            applyCommittedEntries();
        }

        send(channel, new AppendEntriesResponse(state.getCurrentTerm(), true, selfId));
    }

    @Override
    public void onAppendEntriesResponse(AppendEntriesResponse resp) {
        log.debug("Received AppendEntries response: term={}, success={}", resp.getTerm(), resp.isSuccess());

        if (resp.getTerm() > state.getCurrentTerm()) {
            stepDown(resp.getTerm());
            return;
        }

        if (!state.isLeader()) {
            return;
        }

        replicationManager.handleAppendResponse(resp.getNodeId(), resp);
        replicationManager.advanceCommitIndex(resp.getNodeId());
        applyCommittedEntries();
    }

    // ================================================================
    // RequestVote
    // ================================================================

    @Override
    public void onRequestVoteRequest(RequestVoteRequest req, Channel channel) {
        log.debug("Received RequestVote: term={}, candidateId={}, lastLogIndex={}, lastLogTerm={}",
                req.getTerm(), req.getCandidateId(), req.getLastLogIndex(), req.getLastLogTerm());

        if (req.getTerm() > state.getCurrentTerm()) {
            stepDown(req.getTerm());
        }

        boolean grant = false;

        if (req.getTerm() < state.getCurrentTerm()) {
            grant = false;
        } else if (state.getVotedFor() != null && !state.getVotedFor().equals(req.getCandidateId())) {
            grant = false;
        } else if (!electionManager.shouldGrantVote(req.getLastLogTerm(), req.getLastLogIndex())) {
            grant = false;
        } else {
            grant = true;
            state.setVotedFor(req.getCandidateId());
            timerManager.resetElectionTimer(); // granted vote, so reset our timer
        }

        log.debug("Vote for {} in term {}: {}", req.getCandidateId(), req.getTerm(), grant);
        send(channel, new RequestVoteResponse(state.getCurrentTerm(), grant));
    }

    @Override
    public void onRequestVoteResponse(RequestVoteResponse resp) {
        log.debug("Received RequestVote response: term={}, granted={}", resp.getTerm(), resp.isVoteGranted());

        if (resp.getTerm() > state.getCurrentTerm()) {
            stepDown(resp.getTerm());
            return;
        }

        if (!state.isCandidate() || state.getCurrentTerm() != candidateTerm) {
            return; // stale response
        }

        if (resp.isVoteGranted()) {
            voteGrants.add("voter"); // track count
        }

        int grantedCount = voteGrants.size();
        log.debug("Vote count: {}/{}", grantedCount, majorityCount);

        if (grantedCount >= majorityCount) {
            becomeLeader();
        }
    }

    // ================================================================
    // Role transitions
    // ================================================================

    private void stepDown(int newTerm) {
        if (newTerm > state.getCurrentTerm()) {
            log.info("Stepping down: new term {} > current term {}", newTerm, state.getCurrentTerm());
            state.setCurrentTerm(newTerm);
        }
        state.setRole(NodeRole.FOLLOWER);
        state.setVotedFor(null);
        state.setLeaderId(null);
        state.clearLeaderState();
        timerManager.stopHeartbeat();
        timerManager.resetElectionTimer();
        voteGrants.clear();
    }

    private void becomeLeader() {
        log.info("Becoming LEADER for term {}", state.getCurrentTerm());
        state.setRole(NodeRole.LEADER);
        state.setLeaderId(selfId);
        state.clearLeaderState();
        timerManager.stopHeartbeat();
        timerManager.cancelElectionTimer();
        voteGrants.clear();

        // Append a no-op entry to establish leadership and prevent stale commits
        LogEntry noop = logManager.append(state.getCurrentTerm(), "");
        log.info("Appended no-op entry at index {}", noop.getIndex());

        // Initialize replication state
        replicationManager.initialize();

        // Start sending heartbeats immediately
        timerManager.startHeartbeat();

        // Replicate the no-op entry
        replicationManager.replicateLog(noop);
    }

    // ================================================================
    // Heartbeat
    // ================================================================

    private void sendHeartbeat() {
        if (!state.isLeader()) {
            return;
        }
        replicationManager.sendHeartbeat();
    }

    // ================================================================
    // Client Write
    // ================================================================

    @Override
    public void onClientWriteRequest(ClientWriteRequest req, Channel channel) {
        if (!state.isLeader()) {
            String hint = state.getLeaderId();
            send(channel, ClientWriteResponse.redirect(hint));
            return;
        }

        // Append to log
        LogEntry entry = logManager.append(state.getCurrentTerm(), req.getCommand());
        log.info("Appended client write at index {}: {}", entry.getIndex(), req.getCommand());

        // Replicate to peers
        replicationManager.replicateLog(entry);

        // Wait for commit (simplified: respond immediately for now;
        // a real implementation would wait for majority ack)
        // For now, respond success to the client
        send(channel, ClientWriteResponse.ok("appended at index " + entry.getIndex()));
    }

    // ================================================================
    // Client Read
    // ================================================================

    @Override
    public void onClientReadRequest(ClientReadRequest req, Channel channel) {
        if (!state.isLeader()) {
            String hint = state.getLeaderId();
            send(channel, ClientReadResponse.redirect(hint));
            return;
        }

        String value = stateMachine.get(req.getKey());
        send(channel, ClientReadResponse.ok(req.getKey(), value));
    }

    // ================================================================
    // Apply committed entries to state machine
    // ================================================================

    private void applyCommittedEntries() {
        for (LogEntry entry : logManager.getUnappliedEntries()) {
            if (!entry.isNoOp()) {
                stateMachine.apply(entry.getCommand());
                log.debug("Applied entry {} to state machine: {}", entry.getIndex(), entry.getCommand());
            }
            logManager.setLastApplied(entry.getIndex());
        }
    }

    // ================================================================
    // Helpers
    // ================================================================

    private void send(Channel channel, RpcMessage msg) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(msg);
        }
    }

    // Public getters for RaftNode
    public NodeState getState() { return state; }
    public LogManager getLogManager() { return logManager; }
    public StateMachine getStateMachine() { return stateMachine; }
}
