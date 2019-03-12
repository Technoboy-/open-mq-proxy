package com.owl.mq.proxy.registry;

/**
 * @Author: Tboy
 */
public class ServerRegistry {

    private final RegistryService registryService;

    private RegisterMetadata localRegisterMetadata;

    public ServerRegistry(RegistryService registryService){
        this.registryService = registryService;
    }

    public void register(RegisterMetadata registerMetadata){
        this.localRegisterMetadata = registerMetadata;
        this.registryService.register(registerMetadata);
    }

    public void unregister(){
        this.unregister(localRegisterMetadata);
    }

    public void unregister(RegisterMetadata registerMetadata){
        this.registryService.unregister(registerMetadata);
    }

}
