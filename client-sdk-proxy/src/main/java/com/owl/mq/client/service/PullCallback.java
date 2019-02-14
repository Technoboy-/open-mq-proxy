package com.owl.mq.client.service;


import com.owl.mq.client.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface PullCallback {

    void onComplete(Packet response);

    void onException(Throwable ex);
}
