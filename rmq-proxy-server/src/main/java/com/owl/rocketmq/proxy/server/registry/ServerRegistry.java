package com.owl.rocketmq.proxy.server.registry;

import com.owl.client.common.util.NetUtils;
import com.owl.mq.proxy.registry.RegisterMetadata;
import com.owl.mq.proxy.registry.RegistryService;
import com.owl.mq.proxy.transport.Address;


/**
 * @Author: Tboy
 */
public class ServerRegistry {

    private final RegistryService registryService;

    public ServerRegistry(RegistryService registryService){
        this.registryService = registryService;
    }

    public void register(){
        Address address = new Address(NetUtils.getLocalIp(), ServerConfigs.I.getServerPort());
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_PROVIDERS, ServerConfigs.I.getServerTopic()));
        registerMetadata.setAddress(address);
        this.register(registerMetadata);
    }

    public void register(RegisterMetadata registerMetadata){
        this.registryService.register(registerMetadata);
    }

    public void unregister(){
        Address address = new Address(NetUtils.getLocalIp(), ServerConfigs.I.getServerPort());
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_PROVIDERS, ServerConfigs.I.getServerTopic()));
        registerMetadata.setAddress(address);
        this.unregister(registerMetadata);
    }

    public void unregister(RegisterMetadata registerMetadata){
        this.registryService.unregister(registerMetadata);
    }

}
