package com.owl.rocketmq.client.proxy.service;

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
