package com.owl.client.proxy.service;

/**
 * @Author: Tboy
 */
public interface RetryPolicy {

    boolean allowRetry() throws InterruptedException;

    void reset();

}
