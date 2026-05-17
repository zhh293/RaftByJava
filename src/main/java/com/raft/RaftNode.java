package com.raft;

import com.raft.config.ConfigLoader;
import com.raft.config.RaftConfig;
import com.raft.core.*;
import com.raft.rpc.*;
import com.raft.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final RaftConfig config;

    // Infrastructure
    private ScheduledExecutorService raftExecutor;
    private RaftNettyServer nettyServer;
    private RaftNettyClient nettyClient;
    private PeerConnectionManager peerManager;
    private RaftMessageHandler messageHandler;

    // Core
    private NodeState nodeState;
    private LogManager logManager;
    private StateMachine stateMachine;
    private TimerManager timerManager;
    private RaftCore raftCore;
    private PersistenceManager persistenceManager;
    private SnapshotManager snapshotManager;
    private ClientSessionTable sessionTable;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public RaftNode(RaftConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        log.info("Node {} starting on {}:{}", config.getNodeId(),
                config.getListenHost(), config.getListenPort());
        log.info("Cluster size: {}, majority count: {}", config.getClusterSize(), config.getMajorityCount());
        log.info("Data directory: {}", config.getDataDir());

        // 1. Create the Raft core thread (single-threaded executor)
        raftExecutor = Threads.singleThreadScheduledExecutor("raft-core-" + config.getNodeId());

        // 2. Create persistence manager
        persistenceManager = new PersistenceManager(config.getDataDir());

        // 3. Create core components
        nodeState = new NodeState(config.getNodeId());
        nodeState.setPersistenceManager(persistenceManager);

        logManager = new LogManager();
        logManager.setPersistenceManager(persistenceManager);

        stateMachine = new StateMachine();
        sessionTable = new ClientSessionTable();

        // 4. Create snapshot manager
        snapshotManager = new SnapshotManager(
                persistenceManager.getDataDir(), config.getSnapshotThreshold());

        // 5. Recover from persistence
        recoverFromDisk();

        // 6. Create Netty infrastructure
        nettyClient = new RaftNettyClient(null); // handler will be set after creation
        nettyClient.start();

        peerManager = new PeerConnectionManager(config.getNodeId(), nettyClient);
        messageHandler = new RaftMessageHandler(peerManager, raftExecutor);

        // 7. Create timer manager (uses the raft executor for callbacks)
        timerManager = new TimerManager(raftExecutor, config);

        // 8. Create RaftCore — the heart of the system
        raftCore = new RaftCore(config, nodeState, logManager, stateMachine,
                timerManager, peerManager, snapshotManager, sessionTable);

        // 9. Wire the delegate back to the handler
        messageHandler.setCoreDelegate(raftCore);

        // 10. Start Netty server
        nettyServer = new RaftNettyServer(config.getListenHost(), config.getListenPort(), messageHandler);
        nettyServer.start();

        // 11. Connect to peers (async, non-blocking)
        peerManager.connectToPeers(config.getOtherPeers());

        // 12. Initialize Raft core (set follower, start election timer)
        raftExecutor.execute(raftCore::initialize);

        log.info("Node {} started successfully", config.getNodeId());
    }

    /**
     * Recover persisted state from disk:
     * 1. Load snapshot if available (restore state machine)
     * 2. Load WAL entries (replay committed entries after snapshot)
     * 3. Load meta (currentTerm + votedFor)
     */
    private void recoverFromDisk() {
        // Load snapshot first
        boolean hasSnapshot = snapshotManager.loadSnapshot();
        if (hasSnapshot) {
            stateMachine.restoreFromSnapshot(snapshotManager.getLastSnapshotData());
            logManager.applySnapshot(
                    snapshotManager.getLastIncludedIndex(),
                    snapshotManager.getLastIncludedTerm());
            log.info("Recovered snapshot: lastIndex={}, lastTerm={}",
                    snapshotManager.getLastIncludedIndex(),
                    snapshotManager.getLastIncludedTerm());
        }

        // Load WAL entries
        logManager.loadFromDisk();
        log.info("Recovered {} log entries from WAL", logManager.size());

        // Load meta
        PersistenceManager.MetaData meta = persistenceManager.loadMeta();
        if (meta != null) {
            // Set directly to avoid re-persisting what we just loaded
            nodeState.setCurrentTerm(meta.getCurrentTerm());
            nodeState.setVotedFor(meta.getVotedFor());
            log.info("Recovered meta: term={}, votedFor={}", meta.getCurrentTerm(), meta.getVotedFor());
        }
    }

    public void shutdown() {
        log.info("Node {} shutting down", config.getNodeId());
        try {
            if (timerManager != null) timerManager.shutdown();
            if (nettyServer != null) nettyServer.shutdown();
            if (nettyClient != null) nettyClient.shutdown();
            if (raftExecutor != null) raftExecutor.shutdown();
        } finally {
            shutdownLatch.countDown();
        }
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    public RaftConfig getConfig() { return config; }
    public RaftCore getRaftCore() { return raftCore; }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar raft.jar <config-file>");
            System.exit(1);
        }
        RaftConfig config = ConfigLoader.load(args[0]);
        RaftNode node = new RaftNode(config);
        node.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            node.shutdown();
        }, "shutdown-hook"));

        node.awaitShutdown();
    }
}
