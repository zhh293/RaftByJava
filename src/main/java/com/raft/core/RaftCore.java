package com.raft.core;

import com.raft.config.PeerConfig;
import com.raft.config.RaftConfig;
import com.raft.rpc.PeerConnectionManager;
import com.raft.rpc.RaftMessageHandler.RaftCoreDelegate;
import com.raft.rpc.message.*;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The heart of the Raft implementation. All Raft algorithm logic lives here,
 * running on a single dedicated thread (the raft executor).
 * <p>
 * Supports:
 * - Leader election with Pre-Vote
 * - Log replication with majority commit waiting
 * - Linearizable reads via ReadIndex
 * - Client idempotency via session table
 * - Log compaction and snapshots
 * - Single-node membership changes
 */
public class RaftCore implements RaftCoreDelegate {
    private static final Logger log = LoggerFactory.getLogger(RaftCore.class);

    private final RaftConfig config;
    private final NodeState state;
    private final LogManager logManager;
    private final StateMachine stateMachine;
    private final TimerManager timerManager;
    private final ElectionManager electionManager;
    private final ReplicationManager replicationManager;
    private final PeerConnectionManager peerManager;
    private final SnapshotManager snapshotManager;
    private final ClientSessionTable sessionTable;
    private final String selfId;
    private int majorityCount;

    // --- candidate state ---
    private final Set<String> voteGrants = new HashSet<>();
    private int voteRejects = 0;
    private int candidateTerm = -1;

    // --- Pre-Vote state ---
    private final Set<String> preVoteGrants = new HashSet<>();
    private int preVoteTerm = -1;
    private boolean preVoteInProgress = false;

    // --- pending writes: logIndex -> PendingWrite ---
    private final Map<Integer, PendingWrite> pendingWrites = new LinkedHashMap<>();

    // --- pending reads for ReadIndex linearizable reads ---
    private final List<PendingRead> pendingReads = new ArrayList<>();
    private int readIndexHeartbeatAcks = 0;

    // --- membership change tracking ---
    private boolean membershipChangeInProgress = false;

    // --- forwarded writes: track original client channels for forwarded write requests ---
    private final Map<String, Channel> forwardedWriteChannels = new HashMap<>();

    public RaftCore(RaftConfig config,
                    NodeState state,
                    LogManager logManager,
                    StateMachine stateMachine,
                    TimerManager timerManager,
                    PeerConnectionManager peerManager,
                    SnapshotManager snapshotManager,
                    ClientSessionTable sessionTable) {
        this.config = config;
        this.state = state;
        this.logManager = logManager;
        this.stateMachine = stateMachine;
        this.timerManager = timerManager;
        this.peerManager = peerManager;
        this.snapshotManager = snapshotManager;
        this.sessionTable = sessionTable;
        this.selfId = config.getNodeId();
        this.majorityCount = config.getMajorityCount();

        this.electionManager = new ElectionManager(state, logManager, peerManager, config);
        this.replicationManager = new ReplicationManager(state, logManager, peerManager, config);
        this.replicationManager.setSnapshotManager(snapshotManager);

        // Wire up timer callbacks
        timerManager.setElectionTimeoutCallback(this::onElectionTimeout);
        timerManager.setCampaignTimeoutCallback(this::onCampaignTimeout);
        timerManager.setHeartbeatCallback(this::sendHeartbeat);
    }

    // ================================================================
    // Initialization
    // ================================================================

    /**
     * Initialize the Raft core. If persistence is available, restore state
     * from disk; otherwise start fresh.
     */
    public void initialize() {
        log.info("RaftCore initializing as FOLLOWER");
        state.setRole(NodeRole.FOLLOWER);

        // Persistence recovery is handled by RaftNode before calling initialize.
        // If no persisted state exists, NodeState starts at term=0, votedFor=null.

        timerManager.resetElectionTimer();
    }

    // ================================================================
    // Election timeout — now with Pre-Vote
    // ================================================================

    private void onElectionTimeout() {
        if (state.isLeader()) {
            return; // leaders don't time out for election
        }
        log.info("Election timeout fired. Current role: {}, term: {}",
                state.getRole(), state.getCurrentTerm());

        // Start Pre-Vote phase instead of real election
        startPreVote();
    }

    /**
     * Pre-Vote: ask peers if they would vote for us WITHOUT incrementing the term.
     * Only if a majority responds positively do we proceed with a real election.
     */
    private void startPreVote() {
        preVoteInProgress = true;
        preVoteTerm = state.getCurrentTerm() + 1;
        preVoteGrants.clear();
        preVoteGrants.add(selfId); // vote for self

        log.info("Starting Pre-Vote for proposed term {}", preVoteTerm);

        int lastLogIndex = logManager.lastLogIndex();
        int lastLogTerm = logManager.lastLogTerm();

        PreVoteRequest request = new PreVoteRequest(
                preVoteTerm, selfId, lastLogIndex, lastLogTerm);
        peerManager.broadcast(request);
        timerManager.resetElectionTimer();
    }

    /**
     * Real election: called after Pre-Vote succeeds.
     */
    private void startRealElection() {
        preVoteInProgress = false;
        electionManager.startElection();
        candidateTerm = state.getCurrentTerm();
        voteGrants.clear();
        voteGrants.add(selfId);
        voteRejects = 0;
        // Cancel the election timer (it's for followers); start the campaign timer instead
        timerManager.cancelElectionTimer();
        timerManager.startCampaignTimer();
    }

    /**
     * Campaign timeout: the candidate failed to collect a majority within the
     * allotted time. Step back to follower and let the election timer re-fire.
     */
    private void onCampaignTimeout() {
        if (!state.isCandidate()) {
            return;
        }
        log.info("Campaign timeout for term {}, reverting to FOLLOWER", state.getCurrentTerm());
        state.setRole(NodeRole.FOLLOWER);
        voteGrants.clear();
        voteRejects = 0;
        timerManager.cancelCampaignTimer();
        timerManager.resetElectionTimer();
    }

    // ================================================================
    // Pre-Vote handling
    // ================================================================

    @Override
    public void onPreVoteRequest(PreVoteRequest req, Channel channel) {
        log.debug("Received PreVote: nextTerm={}, candidateId={}", req.getNextTerm(), req.getCandidateId());

        boolean grant = false;

        // Grant pre-vote if:
        // 1. The candidate's proposed term >= our current term
        // 2. The candidate's log is at least as up-to-date as ours
        // 3. We don't have a current leader (or our election timer has expired)
        if (req.getNextTerm() >= state.getCurrentTerm()
                && electionManager.shouldGrantVote(req.getLastLogTerm(), req.getLastLogIndex())) {
            grant = true;
        }

        log.debug("Pre-Vote for {} in proposed term {}: {}", req.getCandidateId(), req.getNextTerm(), grant);
        send(channel, new PreVoteResponse(state.getCurrentTerm(), grant));
    }

    @Override
    public void onPreVoteResponse(PreVoteResponse resp) {
        log.debug("Received PreVote response: term={}, granted={}", resp.getTerm(), resp.isVoteGranted());

        if (!preVoteInProgress || resp.getTerm() > state.getCurrentTerm()) {
            if (resp.getTerm() > state.getCurrentTerm()) {
                stepDown(resp.getTerm());
            }
            return;
        }

        if (resp.isVoteGranted()) {
            preVoteGrants.add("pre-voter-" + preVoteGrants.size());
        }

        int grantedCount = preVoteGrants.size();
        log.debug("Pre-Vote count: {}/{}", grantedCount, majorityCount);

        if (grantedCount >= majorityCount) {
            log.info("Pre-Vote succeeded, starting real election for term {}", preVoteTerm);
            preVoteInProgress = false;
            startRealElection();
        }
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
        log.debug("Received AppendEntries response: term={}, success={}, nodeId={}",
                resp.getTerm(), resp.isSuccess(), resp.getNodeId());

        if (resp.getTerm() > state.getCurrentTerm()) {
            stepDown(resp.getTerm());
            return;
        }

        if (!state.isLeader()) {
            return;
        }

        replicationManager.handleAppendResponse(resp.getNodeId(), resp);
        int oldCommit = logManager.getCommitIndex();
        replicationManager.advanceCommitIndex(resp.getNodeId());
        int newCommit = logManager.getCommitIndex();

        if (newCommit > oldCommit) {
            applyCommittedEntries();
            resolvePendingWrites();
            resolvePendingReads();
        }

        // Count heartbeat acks for ReadIndex
        if (resp.isSuccess() && !pendingReads.isEmpty()) {
            readIndexHeartbeatAcks++;
            if (readIndexHeartbeatAcks + 1 >= majorityCount) { // +1 for self
                resolvePendingReads();
            }
        }
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
            voteGrants.add("voter-" + voteGrants.size());
        } else {
            voteRejects++;
        }

        int grantedCount = voteGrants.size();
        log.debug("Vote count: granted={}/{}, rejected={}", grantedCount, majorityCount, voteRejects);

        if (grantedCount >= majorityCount) {
            timerManager.cancelCampaignTimer();
            becomeLeader();
        } else if (voteRejects >= majorityCount) {
            // Majority rejected — no chance of winning, revert to follower immediately
            log.info("Majority rejected vote request ({} rejects), stepping back to FOLLOWER", voteRejects);
            timerManager.cancelCampaignTimer();
            state.setRole(NodeRole.FOLLOWER);
            voteGrants.clear();
            voteRejects = 0;
            timerManager.resetElectionTimer();
        }
    }

    // ================================================================
    // InstallSnapshot
    // ================================================================

    @Override
    public void onInstallSnapshotRequest(InstallSnapshotRequest req, Channel channel) {
        log.info("Received InstallSnapshot: term={}, lastIncludedIndex={}, lastIncludedTerm={}",
                req.getTerm(), req.getLastIncludedIndex(), req.getLastIncludedTerm());

        if (req.getTerm() < state.getCurrentTerm()) {
            send(channel, new InstallSnapshotResponse(state.getCurrentTerm()));
            return;
        }

        stepDown(req.getTerm());
        state.setLeaderId(req.getLeaderId());
        timerManager.resetElectionTimer();

        // Deserialize session entries from the request
        Map<String, ClientSessionTable.SessionEntry> sessions = new HashMap<>();
        if (req.getSnapshotSessions() != null) {
            for (Map.Entry<String, Map<String, Object>> e : req.getSnapshotSessions().entrySet()) {
                Map<String, Object> vals = e.getValue();
                long seq = ((Number) vals.get("sequenceNumber")).longValue();
                String resp = vals.get("response") != null ? vals.get("response").toString() : null;
                sessions.put(e.getKey(), new ClientSessionTable.SessionEntry(seq, resp));
            }
        }

        // Install the snapshot (including session table)
        snapshotManager.installSnapshot(
                req.getLastIncludedIndex(), req.getLastIncludedTerm(),
                req.getSnapshotData(), sessions,
                stateMachine, logManager, sessionTable);

        send(channel, new InstallSnapshotResponse(state.getCurrentTerm()));
    }

    @Override
    public void onInstallSnapshotResponse(InstallSnapshotResponse resp) {
        log.debug("Received InstallSnapshot response: term={}", resp.getTerm());

        if (resp.getTerm() > state.getCurrentTerm()) {
            stepDown(resp.getTerm());
        }
        // After InstallSnapshot succeeds, the follower's matchIndex is updated
        // to the snapshot's lastIncludedIndex — handled by ReplicationManager
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
        timerManager.cancelCampaignTimer();
        timerManager.resetElectionTimer();
        voteGrants.clear();
        voteRejects = 0;
        preVoteInProgress = false;
        preVoteGrants.clear();

        // Fail all pending writes
        failPendingWrites("leader stepped down");
        pendingReads.clear();

        // Fail any forwarded writes waiting for a leader response
        failForwardedWrites("lost leader connection");
    }

    private void becomeLeader() {
        log.info("Becoming LEADER for term {}", state.getCurrentTerm());
        state.setRole(NodeRole.LEADER);
        state.setLeaderId(selfId);
        state.clearLeaderState();
        timerManager.stopHeartbeat();
        timerManager.cancelElectionTimer();
        voteGrants.clear();
        preVoteInProgress = false;
        membershipChangeInProgress = false;

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
        // Reset heartbeat ack counter for ReadIndex
        readIndexHeartbeatAcks = 0;
        replicationManager.sendHeartbeat();
    }

    // ================================================================
    // Client Write — with majority commit waiting
    // ================================================================

    @Override
    public void onClientWriteRequest(ClientWriteRequest req, Channel channel) {
        if (!state.isLeader()) {
            String leaderId = state.getLeaderId();
            if (leaderId == null) {
                log.warn("Client write rejected: no leader elected yet");
                send(channel, ClientWriteResponse.noLeader());
            } else {
                // Forward the write request to the leader on behalf of the client
                log.debug("Forwarding write request to leader {}", leaderId);
                forwardWriteToLeader(leaderId, req, channel);
            }
            return;
        }

        // Idempotency check
        if (req.getClientId() != null && sessionTable.isDuplicate(req.getClientId(), req.getSequenceNumber())) {
            String cached = sessionTable.getCachedResponse(req.getClientId());
            log.info("Duplicate write from client {}, seq={}, returning cached response",
                    req.getClientId(), req.getSequenceNumber());
            String result = cached != null ? cached : "duplicate";
            if (req.isForwarded()) {
                send(channel, ClientWriteResponse.okForwarded(result, req.getForwardingId()));
            } else {
                send(channel, ClientWriteResponse.ok(result));
            }
            return;
        }

        // Append to log
        LogEntry entry = logManager.append(state.getCurrentTerm(), req.getCommand());
        log.info("Appended client write at index {}: {}", entry.getIndex(), req.getCommand());

        // Track pending write — will be resolved when committed
        pendingWrites.put(entry.getIndex(), new PendingWrite(
                entry.getIndex(), channel, req.getCommand(), req.getForwardingId()));

        // Replicate to peers
        replicationManager.replicateLog(entry);

        // For a single-node cluster, commit immediately
        if (majorityCount <= 1) {
            replicationManager.advanceCommitIndex(null);
            applyCommittedEntries();
            resolvePendingWrites();
        }
    }

    // ================================================================
    // Forwarded write request handling
    // ================================================================

    /**
     * Forward a client write request to the known leader.
     * The original client channel is remembered so we can relay the response.
     */
    private void forwardWriteToLeader(String leaderId, ClientWriteRequest req, Channel clientChannel) {
        String fwdId = selfId + "-fwd-" + System.nanoTime();
        forwardedWriteChannels.put(fwdId, clientChannel);

        ClientWriteRequest forwarded = new ClientWriteRequest(
                req.getClientId(), req.getSequenceNumber(), req.getCommand(), fwdId);
        peerManager.sendToPeer(leaderId, forwarded);
    }

    /**
     * Handle a ClientWriteResponse received from the leader (for forwarded writes).
     * Relay the response back to the original client.
     */
    @Override
    public void onClientWriteResponse(ClientWriteResponse resp) {
        String fwdId = resp.getForwardingId();
        if (fwdId == null) {
            return; // Not a forwarded response, ignore
        }
        Channel clientChannel = forwardedWriteChannels.remove(fwdId);
        if (clientChannel != null) {
            // Relay to the original client — strip the forwardingId
            send(clientChannel, new ClientWriteResponse(
                    resp.isSuccess(), resp.getLeaderHint(), resp.getResult(), null));
            log.debug("Relayed forwarded write response to client, fwdId={}", fwdId);
        }
    }

    /**
     * Fail all forwarded writes waiting for a leader response
     * (e.g. when we step down or lose the leader).
     */
    private void failForwardedWrites(String reason) {
        for (Map.Entry<String, Channel> entry : forwardedWriteChannels.entrySet()) {
            Channel ch = entry.getValue();
            if (ch != null && ch.isActive()) {
                send(ch, ClientWriteResponse.noLeader());
            }
        }
        forwardedWriteChannels.clear();
    }

    // ================================================================
    // Client Read — Linearizable via ReadIndex
    // ================================================================

    @Override
    public void onClientReadRequest(ClientReadRequest req, Channel channel) {
        // ------------------------------------------------------------------
        // Eventual-consistency reads: can be served by any node.
        // The client may specify a minAppliedIndex to enforce a freshness
        // guarantee (monotonic-read). If the node hasn't caught up, it rejects.
        // ------------------------------------------------------------------
        if (!req.isLinearizable()) {
            int lastApplied = logManager.getLastApplied();
            if (req.getMinAppliedIndex() > 0 && lastApplied < req.getMinAppliedIndex()) {
                log.debug("Stale read rejected: appliedIndex={} < minRequired={}",
                        lastApplied, req.getMinAppliedIndex());
                send(channel, ClientReadResponse.stale(
                        req.getKey(), lastApplied, req.getMinAppliedIndex()));
                return;
            }
            String value = stateMachine.get(req.getKey());
            send(channel, ClientReadResponse.ok(req.getKey(), value, lastApplied));
            return;
        }

        // ------------------------------------------------------------------
        // Linearizable reads: must go through the Leader via ReadIndex.
        // ------------------------------------------------------------------
        if (!state.isLeader()) {
            String hint = state.getLeaderId();
            if (hint == null) {
                log.warn("Client linearizable read rejected: no leader elected yet");
                send(channel, ClientReadResponse.noLeader());
            } else {
                send(channel, ClientReadResponse.redirect(hint));
            }
            return;
        }

        // ReadIndex mechanism:
        // 1. Record the current commitIndex as readIndex
        // 2. Send a heartbeat to confirm we're still leader
        // 3. Once majority acks the heartbeat AND appliedIndex >= readIndex, serve the read

        int readIndex = logManager.getCommitIndex();

        // If appliedIndex already covers readIndex, serve immediately
        if (logManager.getLastApplied() >= readIndex) {
            String value = stateMachine.get(req.getKey());
            send(channel, ClientReadResponse.ok(req.getKey(), value, logManager.getLastApplied()));
            return;
        }

        // Otherwise, queue for later resolution
        pendingReads.add(new PendingRead(readIndex, req.getKey(), channel));

        // Reset heartbeat ack counter and send heartbeat
        readIndexHeartbeatAcks = 0;
        replicationManager.sendHeartbeat();
    }

    // ================================================================
    // Membership Change — single-node add/remove
    // ================================================================

    @Override
    public void onMembershipChangeRequest(MembershipChangeRequest req, Channel channel) {
        if (!state.isLeader()) {
            String hint = state.getLeaderId();
            if (hint == null) {
                send(channel, MembershipChangeResponse.fail("NO_LEADER: cluster is electing"));
            } else {
                send(channel, MembershipChangeResponse.redirect(hint));
            }
            return;
        }

        // Only allow one membership change at a time
        if (membershipChangeInProgress) {
            send(channel, MembershipChangeResponse.fail("Another membership change is in progress"));
            return;
        }

        membershipChangeInProgress = true;

        // Construct a special log entry with the config command
        String command;
        if (req.getChangeType() == MembershipChangeRequest.ChangeType.ADD_NODE) {
            command = "CONFIG:ADD:" + req.getTargetNodeId() + ":" + req.getTargetHost() + ":" + req.getTargetPort();
            // Immediately add to our config (Raft single-change safety)
            config.addPeer(new PeerConfig(req.getTargetNodeId(), req.getTargetHost(), req.getTargetPort()));
            majorityCount = config.getMajorityCount();
            // Connect to the new peer
            peerManager.connectToPeers(Collections.singletonList(new PeerConfig(req.getTargetNodeId(), req.getTargetHost(), req.getTargetPort())));
        } else {
            command = "CONFIG:REMOVE:" + req.getTargetNodeId();
            config.removePeer(req.getTargetNodeId());
            majorityCount = config.getMajorityCount();
        }

        LogEntry entry = logManager.append(state.getCurrentTerm(), command);
        log.info("Appended membership change at index {}: {}", entry.getIndex(), command);

        // Track as pending write
        pendingWrites.put(entry.getIndex(), new PendingWrite(entry.getIndex(), channel, command));

        // Re-initialize replication for new peer set
        replicationManager.initialize();
        replicationManager.replicateLog(entry);
    }

    // ================================================================
    // Apply committed entries to state machine
    // ================================================================

    private void applyCommittedEntries() {
        for (LogEntry entry : logManager.getUnappliedEntries()) {
            String command = entry.getCommand();

            if (entry.isNoOp()) {
                // No-op, skip application
            } else if (command.startsWith("CONFIG:")) {
                // Membership change entry — apply config change
                applyConfigChange(command);
            } else {
                // Normal KV command
                stateMachine.apply(command);
                log.debug("Applied entry {} to state machine: {}", entry.getIndex(), command);
            }
            logManager.setLastApplied(entry.getIndex());
        }

        // Check if snapshot compaction is needed
        if (snapshotManager != null && snapshotManager.shouldCompact(logManager.size())) {
            snapshotManager.takeSnapshot(stateMachine, logManager, sessionTable);
        }
    }

    /**
     * Apply a configuration change command to the cluster config.
     * Format: CONFIG:ADD:nodeId:host:port or CONFIG:REMOVE:nodeId
     */
    private void applyConfigChange(String command) {
        String[] parts = command.split(":");
        if (parts.length >= 3 && "ADD".equals(parts[1])) {
            String nodeId = parts[2];
            String host = parts.length > 3 ? parts[3] : "127.0.0.1";
            int port = parts.length > 4 ? Integer.parseInt(parts[4]) : 0;
            log.info("Config change applied: ADD node {}", nodeId);
            // Config was already updated when the entry was created (on leader)
            // On followers, apply it now
            if (!state.isLeader()) {
                config.addPeer(new PeerConfig(nodeId, host, port));
                majorityCount = config.getMajorityCount();
            }
        } else if (parts.length >= 3 && "REMOVE".equals(parts[1])) {
            String nodeId = parts[2];
            log.info("Config change applied: REMOVE node {}", nodeId);
            if (!state.isLeader()) {
                config.removePeer(nodeId);
                majorityCount = config.getMajorityCount();
            }
        }
        membershipChangeInProgress = false;
    }

    // ================================================================
    // Pending write resolution
    // ================================================================

    /**
     * Resolve pending writes whose log entries have been committed.
     */
    private void resolvePendingWrites() {
        int commitIndex = logManager.getCommitIndex();
        Iterator<Map.Entry<Integer, PendingWrite>> it = pendingWrites.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, PendingWrite> entry = it.next();
            if (entry.getKey() <= commitIndex) {
                PendingWrite pw = entry.getValue();
                if (pw.getCommand().startsWith("CONFIG:")) {
                    send(pw.getClientChannel(),
                            MembershipChangeResponse.ok("membership change committed at index " + pw.getLogIndex()));
                } else if (pw.isForwarded()) {
                    // This write was forwarded by a follower — send response with forwardingId
                    // so the follower can relay it back to the original client
                    send(pw.getClientChannel(),
                            ClientWriteResponse.okForwarded(
                                    "committed at index " + pw.getLogIndex(), pw.getForwardingId()));
                } else {
                    send(pw.getClientChannel(),
                            ClientWriteResponse.ok("committed at index " + pw.getLogIndex()));
                }
                it.remove();
            }
        }
    }

    /**
     * Fail all pending writes (e.g. when stepping down from leader).
     */
    private void failPendingWrites(String reason) {
        for (PendingWrite pw : pendingWrites.values()) {
            if (pw.getCommand().startsWith("CONFIG:")) {
                send(pw.getClientChannel(), MembershipChangeResponse.fail(reason));
            } else if (pw.isForwarded()) {
                send(pw.getClientChannel(),
                        ClientWriteResponse.failForwarded(reason, pw.getForwardingId()));
            } else {
                send(pw.getClientChannel(),
                        ClientWriteResponse.noLeader());
            }
        }
        pendingWrites.clear();
        membershipChangeInProgress = false;
    }

    // ================================================================
    // Pending read resolution (ReadIndex)
    // ================================================================

    /**
     * Resolve pending reads whose readIndex has been applied.
     */
    private void resolvePendingReads() {
        int lastApplied = logManager.getLastApplied();
        Iterator<PendingRead> it = pendingReads.iterator();
        while (it.hasNext()) {
            PendingRead pr = it.next();
            if (lastApplied >= pr.readIndex) {
                String value = stateMachine.get(pr.key);
                send(pr.channel, ClientReadResponse.ok(pr.key, value, lastApplied));
                it.remove();
            }
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
    public SnapshotManager getSnapshotManager() { return snapshotManager; }
    public ClientSessionTable getSessionTable() { return sessionTable; }

    // ================================================================
    // PendingRead inner class
    // ================================================================

    private static class PendingRead {
        final int readIndex;
        final String key;
        final Channel channel;

        PendingRead(int readIndex, String key, Channel channel) {
            this.readIndex = readIndex;
            this.key = key;
            this.channel = channel;
        }
    }
}
