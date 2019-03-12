package com.owl.kafka.proxy.server.pull;

import com.owl.client.common.util.StringUtils;
import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.proxy.zookeeper.KafkaZookeeperConfig;
import com.owl.kafka.proxy.server.config.KafkaServerConfigs;
import com.owl.kafka.proxy.server.service.DLQService;
import com.owl.kafka.proxy.server.transport.NettyServer;
import com.owl.kafka.proxy.server.consumer.AcknowledgeMessageListenerPullService;
import com.owl.kafka.proxy.server.consumer.ProxyConsumer;
import com.owl.mq.proxy.registry.RegistryManager;

/**
 * @Author: Tboy
 */
public class PullServer {

    private final ProxyConsumer consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    private final DLQService dlqService;

    private final RegistryManager registryManager;

    public PullServer(){
        String kafkaServerList = KafkaServerConfigs.I.getServerKafkaServerList();
        if(StringUtils.isEmpty(kafkaServerList)){
            kafkaServerList = KafkaZookeeperConfig.getBrokerIds(KafkaServerConfigs.I.getZookeeperServerList(), KafkaServerConfigs.I.getZookeeperNamespace());
        }
        this.registryManager = new RegistryManager(KafkaServerConfigs.I);
        //
        ConsumerConfig consumerConfigs = new ConsumerConfig(kafkaServerList, KafkaServerConfigs.I.getServerTopic(), KafkaServerConfigs.I.getServerGroupId());
        consumerConfigs.setAutoCommit(false);
        this.consumer = new ProxyConsumer(consumerConfigs);
        this.nettyServer = new NettyServer(consumer);

        this.messageListenerService = new AcknowledgeMessageListenerPullService();
        this.consumer.setMessageListenerService(messageListenerService);

        this.dlqService = new DLQService(kafkaServerList, KafkaServerConfigs.I.getServerTopic(), KafkaServerConfigs.I.getServerGroupId());
    }

    public void start(){
        this.nettyServer.start();
        this.consumer.start();
    }

    public void close(){
        this.consumer.close();
        this.nettyServer.close();
        this.dlqService.close();
        this.registryManager.close();
    }
}
