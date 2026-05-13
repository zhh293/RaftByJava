package com.raft.rpc;

import com.raft.config.PeerConfig;
import com.raft.rpc.message.IdentificationMessage;
import com.raft.rpc.message.RpcMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * Manages peer connections: maps nodeId to Channel, handles connect/reconnect.
 */
public class PeerConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(PeerConnectionManager.class);

    private final String selfId;
    private final RaftNettyClient nettyClient;
    private final Map<String, Channel> peerChannels = new ConcurrentHashMap<>();

    /** Callback invoked on the caller's thread when a message is received for RaftCore. */
    private final List<BiConsumer<RpcMessage, Channel>> messageListeners = new CopyOnWriteArrayList<>();

    public PeerConnectionManager(String selfId, RaftNettyClient nettyClient) {
        this.selfId = selfId;
        this.nettyClient = nettyClient;
    }

    public void addMessageListener(BiConsumer<RpcMessage, Channel> listener) {
        messageListeners.add(listener);
    }

    /**
     * Called by RaftMessageHandler when a message is received.
     * IdentificationMessages are handled internally; all others are forwarded to listeners.
     */
    public void onMessageReceived(RpcMessage msg, Channel channel) {
        if (msg instanceof IdentificationMessage) {
            String nodeId = ((IdentificationMessage) msg).getNodeId();
            Channel old = peerChannels.put(nodeId, channel);
            if (old != null && old != channel) {
                old.close();
            }
            log.info("Peer {} identified from channel {}", nodeId, channel.remoteAddress());
            return;
        }
        for (BiConsumer<RpcMessage, Channel> listener : messageListeners) {
            listener.accept(msg, channel);
        }
    }

    /**
     * Called when a channel becomes inactive.
     */
    public void onChannelInactive(Channel channel) {
        peerChannels.values().removeIf(c -> c == channel);
    }

    /**
     * Initiate connections to all configured peers (excluding self).
     */
    public void connectToPeers(List<PeerConfig> peers) {
        for (PeerConfig peer : peers) {
            if (peer.getNodeId().equals(selfId)) {
                continue;
            }
            connect(peer);
        }
    }

    private void connect(PeerConfig peer) {
        ChannelFuture future = nettyClient.connect(peer.getHost(), peer.getPort());
        future.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                log.info("Connected to peer {}:{}", peer.getHost(), peer.getPort());
                f.channel().writeAndFlush(new IdentificationMessage(selfId));
            } else {
                log.warn("Failed to connect to peer {}:{}, will retry", peer.getHost(), peer.getPort());
                // Schedule reconnect with backoff
                f.channel().eventLoop().schedule(() -> connect(peer),
                        1, java.util.concurrent.TimeUnit.SECONDS);
            }
        });
    }

    /**
     * Send a message to a specific peer. No-op if the peer is not connected.
     */
    public void sendToPeer(String peerId, RpcMessage msg) {
        Channel channel = peerChannels.get(peerId);
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(msg);
        } else {
            log.debug("Cannot send {} to {}: channel not connected", msg.getClass().getSimpleName(), peerId);
        }
    }

    /**
     * Broadcast a message to all known peers.
     */
    public void broadcast(RpcMessage msg) {
        for (Map.Entry<String, Channel> entry : peerChannels.entrySet()) {
            Channel ch = entry.getValue();
            if (ch.isActive()) {
                ch.writeAndFlush(msg);
            }
        }
    }

    /**
     * Broadcast a message to all known peers except the specified one.
     */
    public void broadcastExcept(String excludePeerId, RpcMessage msg) {
        for (Map.Entry<String, Channel> entry : peerChannels.entrySet()) {
            if (entry.getKey().equals(excludePeerId)) {
                continue;
            }
            Channel ch = entry.getValue();
            if (ch.isActive()) {
                ch.writeAndFlush(msg);
            }
        }
    }

    public int getConnectedPeerCount() {
        return (int) peerChannels.values().stream().filter(Channel::isActive).count();
    }

    public Map<String, Channel> getPeerChannels() {
        return peerChannels;
    }
}
