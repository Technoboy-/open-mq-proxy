package com.owl.mq.proxy.event;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import java.util.concurrent.Executors;

/**
 * @Author: Tboy
 */
public class EventBusPoster {

    private static final EventBus BUS = new EventBus();

    private static final AsyncEventBus ASYNC_BUS = new AsyncEventBus(Executors.newFixedThreadPool(1));

    private static final EventListener LISTENER = new EventListener();

    static{
        BUS.register(LISTENER);
        ASYNC_BUS.register(LISTENER);
    }

    public static void post(Event event){
        BUS.post(event);
    }

    public static void aysncPost(Event event){
        ASYNC_BUS.post(event);
    }
}
