package com.owl.rocketmq.client.producer;



/**
 * @Author: Tboy
 */
public interface Callback {

    void onSuccess(SendResult result);

    void onException(final Throwable e);
}
