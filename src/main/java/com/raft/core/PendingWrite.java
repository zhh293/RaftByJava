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

    public PendingWrite(int logIndex, Channel clientChannel, String command) {
        this.logIndex = logIndex;
        this.clientChannel = clientChannel;
        this.command = command;
    }

    public int getLogIndex() { return logIndex; }
    public Channel getClientChannel() { return clientChannel; }
    public String getCommand() { return command; }
}
