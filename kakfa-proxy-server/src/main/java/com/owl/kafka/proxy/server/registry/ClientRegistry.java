package com.owl.kafka.proxy.server.registry;

import com.owl.client.common.util.ZookeeperConstants;
import com.owl.kafka.proxy.server.consumer.ServerConfigs;
import com.owl.mq.proxy.registry.RegisterMetadata;
import com.owl.mq.proxy.registry.RegistryService;
import com.owl.mq.proxy.transport.Address;
import com.owl.mq.proxy.transport.Connection;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @Author: Tboy
 *
 */
//TODO 改为定时写zk
public class ClientRegistry {

    private final CopyOnWriteArraySet<Connection> localRegistry = new CopyOnWriteArraySet<>();

    private final RegistryService registryService;

    public ClientRegistry(RegistryService registryService){
        this.registryService = registryService;
    }

    public void register(Connection connection){
        localRegistry.add(connection);
        //
        RegisterMetadata metadata = toRegisterMetadata(connection);
        registryService.register(metadata);

    }

    public void unregister(Connection connection){
        localRegistry.remove(connection.getId().asLongText());
        //
        RegisterMetadata metadata = toRegisterMetadata(connection);
        registryService.unregister(metadata);
    }

    //TODO topic信息从client的注册信息里拿，而不是从server端拿。
    private RegisterMetadata toRegisterMetadata(Connection connection){
        InetSocketAddress remoteAddress = ((InetSocketAddress)connection.getRemoteAddress());
        Address address = new Address(remoteAddress.getHostName(), remoteAddress.getPort());
        RegisterMetadata metadata = new RegisterMetadata();
        metadata.setPath(String.format(ZookeeperConstants.ZOOKEEPER_CONSUMERS, ServerConfigs.I.getServerTopic()));
        metadata.setAddress(address);
        return metadata;
    }

}
