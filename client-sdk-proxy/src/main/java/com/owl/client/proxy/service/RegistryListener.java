package com.owl.client.proxy.service;

import com.owl.client.proxy.transport.Address;

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
