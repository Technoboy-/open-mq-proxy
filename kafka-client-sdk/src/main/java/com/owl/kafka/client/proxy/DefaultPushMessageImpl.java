package com.owl.kafka.client.proxy;

import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.bo.ConfigLoader;
import com.owl.mq.client.service.InvokerPromise;
import com.owl.mq.client.registry.RegistryListener;
import com.owl.mq.client.registry.RegistryService;
import com.owl.mq.client.transport.Address;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.ConnectionManager;
import com.owl.mq.client.transport.NettyClient;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.MessageCodec;
import com.owl.mq.client.util.KafkaPackets;
import com.owl.mq.client.zookeeper.ZookeeperClient;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.proxy.transport.KafkaNettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: Tboy
 */
public class DefaultPushMessageImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPushMessageImpl.class);

    private final RegistryService registryService;

    private final NettyClient nettyClient;

    private final ZookeeperClient zookeeperClient;

    private final String serverList = ClientConfigs.I.getZookeeperServerList();

    private final int sessionTimeoutMs = ClientConfigs.I.getZookeeperSessionTimeoutMs();

    private final int connectionTimeoutMs = ClientConfigs.I.getZookeeperConnectionTimeoutMs();

    public DefaultPushMessageImpl(MessageListenerService messageListenerService){
        this.nettyClient = new KafkaNettyClient(messageListenerService);
        this.zookeeperClient = new ZookeeperClient(serverList, sessionTimeoutMs, connectionTimeoutMs);
        this.registryService = new RegistryService(zookeeperClient);
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyClient.connect(address, true);
                        break;
                    case DELETE:
                        nettyClient.disconnect(address);
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(ClientConfigs.I.ZOOKEEPER_PROVIDERS, ClientConfigs.I.getTopic()));
    }

    public void start(){
        LOGGER.debug("DefaultPushMessageImpl started");
    }

    public Record<byte[], byte[]> view(long msgId){
        Record result = Record.EMPTY;
        try {
            List<String> children = zookeeperClient.getChildren(String.format(ConfigLoader.ZOOKEEPER_CONSUMERS, ClientConfigs.I.getTopic() + "-dlq"));
            ConnectionManager connectionManager = null;
            for(String child : children){
                Address address = Address.parse(child);
                if(address != null){
                    Connection connection = nettyClient.getConnectionManager().getConnection(address);
                    connection.send(KafkaPackets.viewReq(msgId));
                    InvokerPromise promise = new InvokerPromise(msgId, 5000);
                    Packet packet = promise.getResult();
                    if(packet != null && !packet.isBodyEmtpy()){
                        KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
                        return new Record<>(kafkaMessage.getHeader().getMsgId(), kafkaMessage.getHeader().getTopic(),
                                kafkaMessage.getHeader().getPartition(), kafkaMessage.getHeader().getOffset(), kafkaMessage.getKey(), kafkaMessage.getValue(), -1);
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("view msgId : {}, error", msgId, ex);
        }
        return result;
    }

    public void close(){
        this.nettyClient.close();
        this.registryService.close();
        this.zookeeperClient.close();
        LOGGER.debug("DefaultPushMessageImpl closed");
    }

}
