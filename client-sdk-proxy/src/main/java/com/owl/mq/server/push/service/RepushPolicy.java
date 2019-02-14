package com.owl.mq.server.push.service;


import com.owl.mq.client.transport.exceptions.ChannelInactiveException;

/**
 * @Author: Tboy
 */
public interface RepushPolicy<T> {

    void start();

    void repush(T msg) throws InterruptedException, ChannelInactiveException;

    void close();
}
