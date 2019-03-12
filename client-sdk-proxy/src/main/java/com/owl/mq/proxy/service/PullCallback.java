package com.owl.mq.proxy.service;


import com.owl.mq.proxy.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface PullCallback {

    void onComplete(Packet response);

    void onException(Throwable ex);
}
