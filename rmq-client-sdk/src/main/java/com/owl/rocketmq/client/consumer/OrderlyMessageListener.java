package com.owl.rocketmq.client.consumer;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface OrderlyMessageListener extends MessageListener{

    boolean onMessage(List<RocketMQMessage> msgs);

}
