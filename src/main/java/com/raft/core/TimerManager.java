package com.raft.core;

import com.raft.config.RaftConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages election timeout and heartbeat scheduling.
 * All timer callbacks fire on the provided Raft core executor.
 */
public class TimerManager {
    private static final Logger log = LoggerFactory.getLogger(TimerManager.class);
    private static final Random RANDOM = new Random();

    private final ScheduledExecutorService executor;
    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;
    private final int heartbeatIntervalMs;

    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;

    private volatile Runnable electionTimeoutCallback;
    private volatile Runnable heartbeatCallback;

    public TimerManager(ScheduledExecutorService executor, RaftConfig config) {
        this.executor = executor;
        this.electionTimeoutMinMs = config.getElectionTimeoutMinMs();
        this.electionTimeoutMaxMs = config.getElectionTimeoutMaxMs();
        this.heartbeatIntervalMs = config.getHeartbeatIntervalMs();
    }

    public void setElectionTimeoutCallback(Runnable callback) {
        this.electionTimeoutCallback = callback;
    }

    public void setHeartbeatCallback(Runnable callback) {
        this.heartbeatCallback = callback;
    }

    /** Reset the election timer with a random delay in [min, max]. */
    public void resetElectionTimer() {
        cancelElectionTimer();
        int delay = electionTimeoutMinMs + RANDOM.nextInt(electionTimeoutMaxMs - electionTimeoutMinMs + 1);
        electionTimer = executor.schedule(this::onElectionTimeout, delay, TimeUnit.MILLISECONDS);
        log.debug("Election timer reset: {}ms", delay);
    }

    /** Cancel the election timer immediately. */
    public void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
            electionTimer = null;
        }
    }

    /** Start periodic heartbeat (called when becoming leader). */
    public void startHeartbeat() {
        stopHeartbeat();
        heartbeatTimer = executor.scheduleAtFixedRate(
                this::onHeartbeat,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info("Heartbeat started at {}ms interval", heartbeatIntervalMs);
    }

    /** Stop periodic heartbeat (called when stepping down from leader). */
    public void stopHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
            heartbeatTimer = null;
        }
    }

    private void onElectionTimeout() {
        if (electionTimeoutCallback != null) {
            electionTimeoutCallback.run();
        }
    }

    private void onHeartbeat() {
        if (heartbeatCallback != null) {
            heartbeatCallback.run();
        }
    }

    public void shutdown() {
        cancelElectionTimer();
        stopHeartbeat();
    }
}
