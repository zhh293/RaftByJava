package com.raft.rpc;

import com.raft.rpc.message.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * Netty handler that dispatches received RPC messages to the Raft core thread.
 * This handler runs on Netty I/O threads and must not touch Raft state directly.
 */
public class RaftMessageHandler extends SimpleChannelInboundHandler<RpcMessage> {
    private static final Logger log = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final PeerConnectionManager peerManager;
    private final Executor raftExecutor;

    // RaftCore — set after construction to avoid circular dependency
    private volatile RaftCoreDelegate coreDelegate;

    public RaftMessageHandler(PeerConnectionManager peerManager, Executor raftExecutor) {
        this.peerManager = peerManager;
        this.raftExecutor = raftExecutor;
    }

    public void setCoreDelegate(RaftCoreDelegate coreDelegate) {
        this.coreDelegate = coreDelegate;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        // Identification messages are handled synchronously by PeerConnectionManager
        if (msg instanceof IdentificationMessage) {
            peerManager.onMessageReceived(msg, ctx.channel());
            return;
        }

        // All other messages are dispatched to the Raft core thread
        raftExecutor.execute(() -> {
            if (coreDelegate == null) {
                log.warn("No core delegate set, dropping message: {}", msg);
                return;
            }
            try {
                dispatch(msg, ctx.channel());
            } catch (Exception e) {
                log.error("Error handling message: {}", msg, e);
            }
        });
    }

    private void dispatch(RpcMessage msg, Channel channel) {
        if (msg instanceof AppendEntriesRequest) {
            coreDelegate.onAppendEntriesRequest((AppendEntriesRequest) msg, channel);
        } else if (msg instanceof AppendEntriesResponse) {
            coreDelegate.onAppendEntriesResponse((AppendEntriesResponse) msg);
        } else if (msg instanceof RequestVoteRequest) {
            coreDelegate.onRequestVoteRequest((RequestVoteRequest) msg, channel);
        } else if (msg instanceof RequestVoteResponse) {
            coreDelegate.onRequestVoteResponse((RequestVoteResponse) msg);
        } else if (msg instanceof ClientWriteRequest) {
            coreDelegate.onClientWriteRequest((ClientWriteRequest) msg, channel);
        } else if (msg instanceof ClientReadRequest) {
            coreDelegate.onClientReadRequest((ClientReadRequest) msg, channel);
        } else {
            log.warn("Unknown message type: {}", msg.getClass().getSimpleName());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        peerManager.onChannelInactive(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception in channel {}: {}", ctx.channel().remoteAddress(), cause.getMessage());
        ctx.close();
    }

    /**
     * Interface that RaftCore implements to receive messages from the network layer.
     */
    public interface RaftCoreDelegate {
        void onAppendEntriesRequest(AppendEntriesRequest request, Channel channel);
        void onAppendEntriesResponse(AppendEntriesResponse response);
        void onRequestVoteRequest(RequestVoteRequest request, Channel channel);
        void onRequestVoteResponse(RequestVoteResponse response);
        void onClientWriteRequest(ClientWriteRequest request, Channel channel);
        void onClientReadRequest(ClientReadRequest request, Channel channel);
    }
}
