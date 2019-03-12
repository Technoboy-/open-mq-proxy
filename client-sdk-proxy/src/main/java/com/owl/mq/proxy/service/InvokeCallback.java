package com.owl.mq.proxy.service;

/**
 * @Author: Tboy
 */
public interface InvokeCallback {

    void onComplete(final InvokerPromise invokerPromise);
}
