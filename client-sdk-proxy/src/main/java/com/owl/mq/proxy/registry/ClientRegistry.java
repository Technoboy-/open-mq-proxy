package com.owl.mq.proxy.registry;

import com.owl.client.common.util.ZookeeperConstants;
import com.owl.mq.proxy.bo.RegisterContent;
import com.owl.mq.proxy.transport.Address;
import com.owl.mq.proxy.transport.Connection;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 *
 */
//TODO 改为定时写zk。 2. 如果客户端重连，这里需要添加定时器判断connection是否有效，并且移除失效映射。
public class ClientRegistry {

    private final ConcurrentHashMap<Connection, RegisterContent> localRegistry = new ConcurrentHashMap<>();

    private final RegistryService registryService;

    public ClientRegistry(RegistryService registryService){
        this.registryService = registryService;
    }

    public void register(Connection connection, RegisterContent registerContent){
        localRegistry.put(connection, registerContent);
        //
        RegisterMetadata metadata = toRegisterMetadata(connection, registerContent);
        registryService.register(metadata);

    }

    public void unregister(Connection connection){
        RegisterContent registerContent = localRegistry.remove(connection);
        if(registerContent != null){
            RegisterMetadata metadata = toRegisterMetadata(connection, registerContent);
            registryService.unregister(metadata);
        }
    }

    private RegisterMetadata toRegisterMetadata(Connection connection, RegisterContent registerContent){
        InetSocketAddress remoteAddress = ((InetSocketAddress)connection.getRemoteAddress());
        Address address = new Address(remoteAddress.getHostName(), remoteAddress.getPort());
        RegisterMetadata metadata = new RegisterMetadata();
        metadata.setPath(String.format(ZookeeperConstants.ZOOKEEPER_CONSUMERS, registerContent.getTopic()));
        metadata.setAddress(address);
        return metadata;
    }

}
