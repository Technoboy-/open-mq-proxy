package com.owl.rocketmq.client.proxy;


import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.bo.ConfigLoader;
import com.owl.mq.client.registry.RegistryListener;
import com.owl.mq.client.registry.RegistryService;
import com.owl.mq.client.service.InvokerPromise;
import com.owl.mq.client.service.PullMessageService;
import com.owl.mq.client.transport.Address;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.NettyClient;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.MessageCodec;
import com.owl.mq.client.util.KafkaPackets;
import com.owl.mq.client.zookeeper.ZookeeperClient;
import com.owl.rocketmq.client.consumer.service.MessageListenerService;
import com.owl.rocketmq.client.proxy.service.RmqPullMessageService;
import com.owl.rocketmq.client.proxy.transport.RmqNettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: Tboy
 */
public class DefaultPullMessageImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullMessageImpl.class);

    private final RegistryService registryService;

    private final NettyClient nettyClient;

    private final ZookeeperClient zookeeperClient;

    private final PullMessageService pullMessageService;

    private final String serverList = ClientConfigs.I.getZookeeperServerList();

    private final int sessionTimeoutMs = ClientConfigs.I.getZookeeperSessionTimeoutMs();

    private final int connectionTimeoutMs = ClientConfigs.I.getZookeeperConnectionTimeoutMs();

    public DefaultPullMessageImpl(MessageListenerService messageListenerService){
        this.nettyClient = new RmqNettyClient(messageListenerService);
        this.pullMessageService = new RmqPullMessageService(nettyClient);
        this.zookeeperClient = new ZookeeperClient(serverList, sessionTimeoutMs, connectionTimeoutMs);
        this.registryService = new RegistryService(zookeeperClient);
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyClient.connect(address, true);
                        pullMessageService.startPull(address);
                        break;
                    case DELETE:
                        nettyClient.disconnect(address);
                        pullMessageService.stopPull(address);
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(ClientConfigs.I.ZOOKEEPER_PROVIDERS, ClientConfigs.I.getTopic()));
    }

    public void start(){
        LOGGER.debug("DefaultPullMessageImpl started");

    }

    public void close(){
        this.pullMessageService.close();
        this.nettyClient.close();
        this.registryService.close();
        this.zookeeperClient.close();
        LOGGER.debug("DefaultPullMessageImpl closed");
    }

}
