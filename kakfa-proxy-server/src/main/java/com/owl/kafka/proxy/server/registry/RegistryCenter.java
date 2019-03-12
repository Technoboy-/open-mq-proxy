package com.owl.kafka.proxy.server.registry;

import com.owl.kafka.proxy.server.consumer.ServerConfigs;
import com.owl.mq.proxy.registry.RegistryService;
import com.owl.mq.proxy.zookeeper.ZookeeperClient;
import com.owl.mq.server.registry.ClientRegistry;
import com.owl.mq.server.service.InstanceHolder;

/**
 * @Author: Tboy
 */
public class RegistryCenter {

    private final ServerRegistry serverRegistry;

    private final ClientRegistry clientRegistry;

    private final ZookeeperClient zookeeperClient;

    private final RegistryService registryService;

    public RegistryCenter(){
        this.zookeeperClient = new ZookeeperClient(ServerConfigs.I.getZookeeperServerList(),
                ZookeeperClient.PUSH_SERVER_NAMESPACE, ServerConfigs.I.getZookeeperSessionTimeoutMs(),
                ServerConfigs.I.getZookeeperConnectionTimeoutMs());
        this.registryService = new RegistryService(this.zookeeperClient);
        //
        this.serverRegistry = new ServerRegistry(registryService);
        this.clientRegistry = new ClientRegistry(registryService);

        //
        InstanceHolder.I.set(this.zookeeperClient);
        InstanceHolder.I.set(this);
    }

    public ServerRegistry getServerRegistry() {
        return serverRegistry;
    }

    public ClientRegistry getClientRegistry() {
        return clientRegistry;
    }

    public void close(){
        this.serverRegistry.unregister();
        this.registryService.close();
        this.zookeeperClient.close();
    }
}
