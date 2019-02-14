package com.owl.kafka.proxy.server.transport;

import com.owl.mq.client.transport.codec.PacketDecoder;
import com.owl.mq.client.transport.codec.PacketEncoder;
import com.owl.mq.client.transport.handler.MessageDispatcher;
import com.owl.mq.client.transport.protocol.Command;
import com.owl.kafka.proxy.server.consumer.ProxyConsumer;
import com.owl.kafka.proxy.server.transport.handler.*;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.registry.RegistryCenter;
import com.owl.mq.server.service.InstanceHolder;
import com.owl.mq.server.transport.NettyTcpServer;
import com.owl.mq.server.transport.handler.PingMessageHandler;
import com.owl.mq.server.transport.handler.UnregisterMessageHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;


/**
 * @Author: Tboy
 */
public class NettyServer extends NettyTcpServer {

    private final ChannelHandler handler;

    public NettyServer(ProxyConsumer consumer) {
        this.handler = new ServerHandler(newDispatcher(consumer));
    }

    private MessageDispatcher newDispatcher(ProxyConsumer consumer){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.PING, new PingMessageHandler());
        dispatcher.register(Command.UNREGISTER, new UnregisterMessageHandler());
        dispatcher.register(Command.ACK, new AckMessageHandler(consumer));
        dispatcher.register(Command.VIEW_REQ, new ViewReqMessageHandler());
        dispatcher.register(Command.PULL_REQ, new PullReqMessageHandler());
        dispatcher.register(Command.SEND_BACK, new SendBackMessageHandler());
        return dispatcher;
    }

    protected void initTcpOptions(ServerBootstrap bootstrap){
        super.initTcpOptions(bootstrap);
        bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_SNDBUF, 64 * 1024) //64k
                .option(ChannelOption.SO_RCVBUF, 64 * 1024); //64k
    }

    @Override
    protected void afterStart() {
        InstanceHolder.I.get(RegistryCenter.class).getServerRegistry().register();
    }

    protected void initNettyChannel(NioSocketChannel ch) throws Exception{

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("encoder", getEncoder());
        //in
        pipeline.addLast("decoder", getDecoder());
        pipeline.addLast("timeOutHandler", new ReadTimeoutHandler(90));
        pipeline.addLast("handler", getChannelHandler());
    }

    @Override
    protected ChannelHandler getEncoder() {
        return new PacketEncoder();
    }

    @Override
    protected ChannelHandler getDecoder() {
        return new PacketDecoder();
    }

    @Override
    protected ChannelHandler getChannelHandler() {
        return handler;
    }

    public void close(){
        super.close();
    }

}
