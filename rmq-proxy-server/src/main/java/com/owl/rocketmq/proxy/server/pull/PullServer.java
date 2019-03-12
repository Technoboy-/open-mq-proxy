package com.owl.rocketmq.proxy.server.pull;

import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.registry.RegistryCenter;
import com.owl.rocketmq.client.consumer.ConsumerConfig;
import com.owl.rocketmq.client.proxy.service.MessageListenerService;
import com.owl.rocketmq.proxy.server.consumer.AcknowledgeMessageListenerService;
import com.owl.rocketmq.proxy.server.consumer.ProxyConsumer;
import com.owl.rocketmq.proxy.server.transport.NettyServer;


/**
 * @Author: Tboy
 */
public class PullServer {

    private final ProxyConsumer consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    private final RegistryCenter registryCenter;

    public PullServer(){

        this.registryCenter = new RegistryCenter();
        //
        ConsumerConfig consumerConfigs = new ConsumerConfig(ServerConfigs.I.getServerNamesrvList(), ServerConfigs.I.getServerTopic(),
                ServerConfigs.I.getServerTags(), ServerConfigs.I.getServerGroupId());
        this.consumer = new ProxyConsumer(consumerConfigs);
        this.nettyServer = new NettyServer(consumer);

        this.messageListenerService = new AcknowledgeMessageListenerService();
        this.consumer.setMessageListenerService(messageListenerService);
    }

    public void start(){
        this.nettyServer.start();
        this.consumer.start();
    }

    public void close(){
        this.consumer.close();
        this.nettyServer.close();
        this.registryCenter.close();
    }
}
