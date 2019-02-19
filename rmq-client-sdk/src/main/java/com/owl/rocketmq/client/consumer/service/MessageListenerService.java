package com.owl.rocketmq.client.consumer.service;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface MessageListenerService {

    boolean onMessage(List<MessageExt> msgs);

    void close();
}
