package com.raft.rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNettyClient {
    private static final Logger log = LoggerFactory.getLogger(RaftNettyClient.class);

    private final ChannelHandler handler;
    private EventLoopGroup group;

    public RaftNettyClient(ChannelHandler handler) {
        this.handler = handler;
    }

    public void start() {
        group = new NioEventLoopGroup();
    }

    public ChannelFuture connect(String host, int port) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new MessageDecoder());
                        p.addLast(new MessageEncoder());
                        p.addLast(handler);
                    }
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.SO_KEEPALIVE, true);

        return bootstrap.connect(host, port);
    }

    public void shutdown() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}
