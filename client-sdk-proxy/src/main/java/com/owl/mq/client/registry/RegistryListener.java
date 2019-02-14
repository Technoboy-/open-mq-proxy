package com.owl.mq.client.registry;

import com.owl.mq.client.transport.Address;

/**
 * @Author: Tboy
 */
public interface RegistryListener {

    void onChange(Address address, Event event);

    enum Event{

        ADD,

        DELETE;
    }
}
