package com.owl.kafka.proxy.server.transport.handler;

import com.owl.mq.proxy.registry.RegistryManager;
import com.owl.mq.proxy.service.InstanceHolder;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.NettyConnection;
import com.owl.mq.proxy.transport.handler.MessageDispatcher;
import com.owl.mq.proxy.transport.protocol.Packet;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
@Sharable
public class ServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final MessageDispatcher dispatcher;

    public ServerHandler(MessageDispatcher dispatcher){
        this.dispatcher = dispatcher;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        NettyConnection.attachChannel(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Connection connnection = NettyConnection.attachChannel(ctx.channel());
        InstanceHolder.I.get(RegistryManager.class).getClientRegistry().unregister(connnection);
        connnection.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        dispatcher.dispatch(NettyConnection.attachChannel(ctx.channel()), (Packet)msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("exceptionCaught", cause);
        ctx.close();
    }


}
