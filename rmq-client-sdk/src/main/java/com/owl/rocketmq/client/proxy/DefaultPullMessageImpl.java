package com.owl.rocketmq.client.proxy;


import com.owl.client.common.util.ZookeeperConstants;
import com.owl.mq.proxy.registry.RegistryListener;
import com.owl.mq.proxy.registry.RegistryService;
import com.owl.mq.proxy.service.PullMessageService;
import com.owl.mq.proxy.transport.Address;
import com.owl.mq.proxy.transport.NettyClient;
import com.owl.mq.proxy.zookeeper.ZookeeperClient;
import com.owl.rocketmq.client.proxy.service.MessageListenerService;
import com.owl.rocketmq.client.proxy.service.RmqPullMessageService;
import com.owl.rocketmq.client.proxy.transport.RmqNettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class DefaultPullMessageImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullMessageImpl.class);

    private final RegistryService registryService;

    private final NettyClient nettyClient;

    private final ZookeeperClient zookeeperClient;

    private final PullMessageService pullMessageService;

    //RMQ 从哪里获取注册的列表。
    private final String serverList = "";

    private final int sessionTimeoutMs = 0;

    private final int connectionTimeoutMs = 0;

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
        //todo rmq的topic获取
        this.registryService.subscribe(String.format(ZookeeperConstants.ZOOKEEPER_PROVIDERS, ""));
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
