package com.owl.rocketmq.client.consumer.service;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface MessageListenerService {

    void onMessage(MessageQueue messageQueue, List<MessageExt> msgs);

    void close();
}
