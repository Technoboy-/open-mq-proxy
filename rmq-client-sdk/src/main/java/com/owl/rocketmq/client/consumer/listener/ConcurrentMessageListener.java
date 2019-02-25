package com.owl.rocketmq.client.consumer.listener;

import com.owl.rocketmq.client.consumer.RocketMQMessage;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface ConcurrentMessageListener<V> extends MessageListener{

    void onMessage(List<RocketMQMessage<V>> msgs);

}
