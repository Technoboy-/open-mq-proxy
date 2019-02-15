package com.owl.rocketmq.client.consumer.listener;

import com.owl.rocketmq.client.consumer.RocketMQMessage;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface OrderlyMessageListener<V> extends MessageListener{

    boolean onMessage(List<RocketMQMessage<V>> msgs);

}
