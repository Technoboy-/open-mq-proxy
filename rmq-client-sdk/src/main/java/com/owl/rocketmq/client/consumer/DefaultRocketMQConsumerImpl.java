package com.owl.rocketmq.client.consumer;


import com.owl.client.common.serializer.Serializer;
import com.owl.client.common.util.Preconditions;
import com.owl.rocketmq.client.consumer.listener.ConcurrentMessageListener;
import com.owl.rocketmq.client.consumer.listener.MessageListener;
import com.owl.rocketmq.client.consumer.listener.OrderlyMessageListener;
import com.owl.rocketmq.client.consumer.service.MessageListenerService;
import com.owl.rocketmq.client.consumer.service.PullMessageListenerService;
import com.owl.rocketmq.client.proxy.DefaultPullMessageImpl;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class DefaultRocketMQConsumerImpl<V> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRocketMQConsumerImpl.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final DefaultMQPushConsumer consumer;
    private final ConsumerConfig consumerConfig;
    private final Serializer serializer;
    private MessageListener messageListener;

    private MessageListenerService messageListenerService;

    private DefaultPullMessageImpl defaultPullMessageImpl;



    public DefaultRocketMQConsumerImpl(ConsumerConfig consumerConfig){
        this.consumerConfig = consumerConfig;
        this.consumer = new DefaultMQPushConsumer(consumerConfig.getConsumerGroup());
        this.consumer.setMessageModel(consumerConfig.getMessageModel());
        this.consumer.setNamesrvAddr(consumerConfig.getNamesrvAddr());
        this.consumer.setConsumeFromWhere(consumerConfig.getConsumeFromWhere());
        this.consumer.setConsumeMessageBatchMaxSize(consumerConfig.getConsumeMessageBatchMaxSize());
        try {
            this.consumer.subscribe(this.consumerConfig.getTopic(), this.consumerConfig.getTags());
        } catch (Exception ex){
            LOG.error("DefaultRocketMQConsumerImpl subscribe error!", ex);
            throw new RuntimeException(ex);
        }
        this.serializer = consumerConfig.getSerializer();
    }


    public void start() {

        Preconditions.checkArgument(messageListener != null, "messageListener should not be null");
        Preconditions.checkArgument(serializer != null, "serializer should not be null");

        if(start.compareAndSet(false, true)){
            try {
                if(consumerConfig.isUseProxy()){
                    this.defaultPullMessageImpl = new DefaultPullMessageImpl(messageListenerService);
                    this.defaultPullMessageImpl.start();
                } else{
                    if(messageListener instanceof ConcurrentMessageListener){
                        ConcurrentMessageListener concurrentMessageListener = (ConcurrentMessageListener)messageListener;
                        consumer.registerMessageListener(new MessageListenerConcurrently() {
                            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                            ConsumeConcurrentlyContext context) {
                                try {
                                    concurrentMessageListener.onMessage(convert(msgs));
                                } catch (Throwable ex){
                                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                                }
                                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                            }
                        });
                    } else if(messageListener instanceof OrderlyMessageListener){
                        OrderlyMessageListener orderlyMessageListener = (OrderlyMessageListener)messageListener;
                        consumer.registerMessageListener(new MessageListenerOrderly() {

                            @Override
                            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                                try {
                                    orderlyMessageListener.onMessage(convert(msgs));
                                } catch (Throwable ex){
                                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                                }
                                return ConsumeOrderlyStatus.SUCCESS;
                            }
                        });
                    }
                    consumer.start();
                }

                Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

                LOG.info("LOG startupInfo : {}", startupInfo());

            } catch (Exception ex) {
                LOG.error("DefaultRocketMQConsumerImpl start error!", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private List<RocketMQMessage<V>> convert(List<MessageExt> msgs){
        List<RocketMQMessage<V>> messages = new ArrayList<>();
        for(MessageExt ext : msgs){
            RocketMQMessage rmqMessage = new RocketMQMessage(ext);
            rmqMessage.setBody((V)serializer.deserialize(ext.getBody(), Object.class));
            messages.add(rmqMessage);
        }
        return messages;
    }

    public void setMessageListener(MessageListener messageListener) {
        if(this.messageListener != null){
            throw new IllegalArgumentException("messageListener has already set");
        }

        this.messageListener = messageListener;
        if(consumerConfig.isUseProxy()){
            this.messageListenerService = new PullMessageListenerService(this.messageListener);
        }
    }

    public void close() {
        if(start.compareAndSet(true, false)){
            if(this.consumer != null) {
                this.consumer.shutdown();
                LOG.info("DefaultRocketMQConsumerImpl closed !");
            }
            if(this.defaultPullMessageImpl != null){
                this.defaultPullMessageImpl.close();
            }
        }
    }

    private String startupInfo(){
        StringBuilder builder = new StringBuilder(100);
        builder.append("DefaultRocketMQConsumerImpl started! ");
        builder.append("  namesrvAddr : ").append(consumerConfig.getNamesrvAddr());
        builder.append("  topic: ").append(consumerConfig.getTopic());
        builder.append("  tags : ").append(consumerConfig.getTags());
        builder.append("  consumerGroup : ").append(consumerConfig.getConsumerGroup());
        return builder.toString();
    }
}
