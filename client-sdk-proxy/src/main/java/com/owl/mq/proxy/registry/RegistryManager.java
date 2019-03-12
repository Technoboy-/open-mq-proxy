package com.owl.mq.proxy.registry;

import com.owl.mq.proxy.zookeeper.ZookeeperClient;

/**
 * @Author: Tboy
 */
public class RegistryManager {

    private final ServerRegistry serverRegistry;

    private final ClientRegistry clientRegistry;

    private final ZookeeperClient zookeeperClient;

    private final RegistryService registryService;

    public RegistryManager(){
        this.zookeeperClient = new ZookeeperClient(ServerConfigs.I.getZookeeperServerList());
        this.registryService = new RegistryService(this.zookeeperClient);
        //
        this.serverRegistry = new ServerRegistry(registryService);
        this.clientRegistry = new ClientRegistry(registryService);

        //
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
