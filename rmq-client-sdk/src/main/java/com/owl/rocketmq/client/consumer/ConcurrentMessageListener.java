package com.owl.rocketmq.client.consumer;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface ConcurrentMessageListener<V> extends MessageListener{

    boolean onMessage(List<RocketMQMessage<V>> msgs);

}
