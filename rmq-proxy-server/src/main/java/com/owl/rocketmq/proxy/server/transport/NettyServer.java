package com.owl.rocketmq.proxy.server.transport;

import com.owl.client.common.util.NetUtils;
import com.owl.client.common.util.ZookeeperConstants;
import com.owl.mq.proxy.registry.RegisterMetadata;
import com.owl.mq.proxy.registry.RegistryManager;
import com.owl.mq.proxy.service.InstanceHolder;
import com.owl.mq.proxy.transport.Address;
import com.owl.mq.proxy.transport.NettyTcpServer;
import com.owl.mq.proxy.transport.codec.PacketDecoder;
import com.owl.mq.proxy.transport.codec.PacketEncoder;
import com.owl.mq.proxy.transport.handler.MessageDispatcher;
import com.owl.mq.proxy.transport.handler.PingMessageHandler;
import com.owl.mq.proxy.transport.handler.UnregisterMessageHandler;
import com.owl.mq.proxy.transport.protocol.Command;
import com.owl.rocketmq.proxy.server.config.RmqServerConfigs;
import com.owl.rocketmq.proxy.server.consumer.ProxyConsumer;
import com.owl.rocketmq.proxy.server.transport.handler.AckMessageHandler;
import com.owl.rocketmq.proxy.server.transport.handler.PullReqMessageHandler;
import com.owl.rocketmq.proxy.server.transport.handler.SendBackMessageHandler;
import com.owl.rocketmq.proxy.server.transport.handler.ServerHandler;
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

    private static final int port = RmqServerConfigs.I.getServerPort();

    private static final int bossNum = RmqServerConfigs.I.getServerBossNum();

    private static final int workerNum = RmqServerConfigs.I.getServerWorkerNum();

    private final ChannelHandler handler;

    public NettyServer(ProxyConsumer consumer) {
        super(port, bossNum, workerNum);
        this.handler = new ServerHandler(newDispatcher(consumer));
    }

    private MessageDispatcher newDispatcher(ProxyConsumer consumer){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.PING, new PingMessageHandler());
        dispatcher.register(Command.UNREGISTER, new UnregisterMessageHandler());
        dispatcher.register(Command.ACK, new AckMessageHandler(consumer));
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
        Address address = new Address(NetUtils.getLocalIp(), port);
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(ZookeeperConstants.ZOOKEEPER_PROVIDERS, RmqServerConfigs.I.getServerTopic()));
        registerMetadata.setAddress(address);

        InstanceHolder.I.get(RegistryManager.class).getServerRegistry().register(registerMetadata);
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
