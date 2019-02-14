package com.owl.mq.client.service;

/**
 * @Author: Tboy
 */
public interface InvokeCallback {

    void onComplete(final InvokerPromise invokerPromise);
}
