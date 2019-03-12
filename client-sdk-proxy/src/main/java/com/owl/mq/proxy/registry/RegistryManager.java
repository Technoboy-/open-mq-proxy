package com.owl.mq.proxy.registry;

import com.owl.mq.proxy.bo.ServerConfigs;
import com.owl.mq.proxy.service.InstanceHolder;
import com.owl.mq.proxy.zookeeper.ZookeeperClient;

/**
 * @Author: Tboy
 */
public class RegistryManager {

    private final ServerRegistry serverRegistry;

    private final ClientRegistry clientRegistry;

    private final ZookeeperClient zookeeperClient;

    private final RegistryService registryService;

    public RegistryManager(ServerConfigs serverConfigs){
        this.zookeeperClient = new ZookeeperClient(serverConfigs.getZookeeperServerList());
        this.registryService = new RegistryService(this.zookeeperClient);
        //
        this.serverRegistry = new ServerRegistry(registryService);
        this.clientRegistry = new ClientRegistry(registryService);

        InstanceHolder.I.set(this);
        InstanceHolder.I.set(this.zookeeperClient);

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
