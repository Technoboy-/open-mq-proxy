package com.owl.rocketmq.client.consumer;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface ConcurrentMessageListener extends MessageListener{

    boolean onMessage(List<RocketMQMessage> msgs);

}
