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

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public RaftNode(RaftConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        log.info("Node {} starting on {}:{}", config.getNodeId(),
                config.getListenHost(), config.getListenPort());
        log.info("Cluster size: {}, majority count: {}",
                config.getClusterSize(), config.getMajorityCount());

        // 1. Create the Raft core thread (single-threaded executor)
        raftExecutor = Threads.singleThreadScheduledExecutor("raft-core-" + config.getNodeId());

        // 2. Create core components
        nodeState = new NodeState(config.getNodeId());
        logManager = new LogManager();
        stateMachine = new StateMachine();

        // 3. Create Netty infrastructure
        nettyClient = new RaftNettyClient(null); // handler will be set after creation
        nettyClient.start();

        peerManager = new PeerConnectionManager(config.getNodeId(), nettyClient);
        messageHandler = new RaftMessageHandler(peerManager, raftExecutor);

        // 4. Create timer manager (uses the raft executor for callbacks)
        timerManager = new TimerManager(raftExecutor, config);

        // 5. Create RaftCore — the heart of the system
        raftCore = new RaftCore(config, nodeState, logManager, stateMachine, timerManager, peerManager);

        // 6. Wire the delegate back to the handler
        messageHandler.setCoreDelegate(raftCore);

        // 7. Start Netty server
        nettyServer = new RaftNettyServer(config.getListenHost(), config.getListenPort(), messageHandler);
        nettyServer.start();

        // 8. Connect to peers (async, non-blocking)
        peerManager.connectToPeers(config.getOtherPeers());

        // 9. Initialize Raft core (set follower, start election timer)
        raftExecutor.execute(raftCore::initialize);

        log.info("Node {} started successfully", config.getNodeId());
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
