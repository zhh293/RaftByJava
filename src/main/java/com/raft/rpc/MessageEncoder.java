package com.raft.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raft.rpc.message.RpcMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageEncoder extends MessageToByteEncoder<RpcMessage> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage msg, ByteBuf out) throws Exception {
        byte[] bytes = MAPPER.writeValueAsBytes(msg);
        out.writeInt(bytes.length);
        out.writeBytes(bytes);
    }
}
