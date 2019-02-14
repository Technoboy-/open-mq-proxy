package com.owl.mq.server.push.service;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface LoadBalance<T> {

    T select(List<T> invokers);
}
