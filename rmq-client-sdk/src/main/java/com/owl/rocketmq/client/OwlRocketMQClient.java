package com.owl.rocketmq.client;

import com.owl.rocketmq.client.consumer.ConsumerConfig;
import com.owl.rocketmq.client.consumer.DefaultRocketMQConsumerImpl;
import com.owl.rocketmq.client.producer.DefaultRocketMQProducerImpl;
import com.owl.rocketmq.client.producer.ProducerConfig;

/**
 * @Author: Tboy
 */
public class OwlRocketMQClient {

    public static DefaultRocketMQConsumerImpl createConsumer(ConsumerConfig configs)
    {
        return new DefaultRocketMQConsumerImpl(configs);
    }

    public static DefaultRocketMQProducerImpl createProducer(ProducerConfig configs){
        return new DefaultRocketMQProducerImpl(configs);
    }
}
