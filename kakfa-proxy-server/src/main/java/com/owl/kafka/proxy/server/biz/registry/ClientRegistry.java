package com.owl.kafka.proxy.server.biz.registry;

import com.owl.kafka.client.proxy.service.RegisterMetadata;
import com.owl.kafka.client.proxy.service.RegistryService;
import com.owl.kafka.client.proxy.transport.Address;
import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @Author: Tboy
 */
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

    private RegisterMetadata toRegisterMetadata(Connection connection){
        InetSocketAddress remoteAddress = ((InetSocketAddress)connection.getRemoteAddress());
        Address address = new Address(remoteAddress.getHostName(), remoteAddress.getPort());
        RegisterMetadata metadata = new RegisterMetadata();
        metadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_CONSUMERS, ServerConfigs.I.getServerTopic()));
        metadata.setAddress(address);
        return metadata;
    }

    public List<Connection> getClients(){
        return new ArrayList<>(localRegistry);
    }

}
