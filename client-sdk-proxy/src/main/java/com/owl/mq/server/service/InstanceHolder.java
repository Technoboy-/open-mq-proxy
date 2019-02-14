package com.owl.mq.server.service;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class InstanceHolder {

    public static InstanceHolder I = new InstanceHolder();

    private final ConcurrentHashMap<String, Object> instances = new ConcurrentHashMap<>();

    public <T> void set(T t){
        instances.put(t.getClass().getName(), t);
    }

    public <T> T get(Class<T> clazz){
        return (T)instances.get(clazz.getName());
    }
}
