package com.raft.core;

import io.netty.channel.Channel;

/**
 * Tracks a pending client write request awaiting majority commit.
 * Created when the leader appends a client command to its log,
 * and resolved when the entry is committed (or the leader steps down).
 */
public class PendingWrite {
    private final int logIndex;
    private final Channel clientChannel;
    private final String command;
    /** Non-null when this write was forwarded from a follower. */
    private final String forwardingId;

    public PendingWrite(int logIndex, Channel clientChannel, String command) {
        this(logIndex, clientChannel, command, null);
    }

    public PendingWrite(int logIndex, Channel clientChannel, String command, String forwardingId) {
        this.logIndex = logIndex;
        this.clientChannel = clientChannel;
        this.command = command;
        this.forwardingId = forwardingId;
    }

    public int getLogIndex() { return logIndex; }
    public Channel getClientChannel() { return clientChannel; }
    public String getCommand() { return command; }
    public String getForwardingId() { return forwardingId; }
    public boolean isForwarded() { return forwardingId != null; }
}
