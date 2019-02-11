package com.owl.rocketmq.client.consumer;


import com.owl.rocketmq.client.util.Preconditions;
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
public class DefaultRocketMQConsumerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRocketMQConsumerImpl.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final DefaultMQPushConsumer consumer;
    private final ConsumerConfig consumerConfig;
    private MessageListener messageListener;

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
    }


    public void start() {

        Preconditions.checkArgument(messageListener != null, "messageListener should not be null");

        if(start.compareAndSet(false, true)){
            try {
                if(messageListener instanceof ConcurrentMessageListener){
                    ConcurrentMessageListener concurrentMessageListener = (ConcurrentMessageListener)messageListener;
                    consumer.registerMessageListener(new MessageListenerConcurrently() {
                        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                        ConsumeConcurrentlyContext context) {
                            boolean ret = concurrentMessageListener.onMessage(convert(msgs));
                            return ret ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS : ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    });
                } else if(messageListener instanceof OrderlyMessageListener){
                    OrderlyMessageListener orderlyMessageListener = (OrderlyMessageListener)messageListener;
                    consumer.registerMessageListener(new MessageListenerOrderly() {

                        @Override
                        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                            boolean ret = orderlyMessageListener.onMessage(convert(msgs));
                            return ret ? ConsumeOrderlyStatus.SUCCESS : ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }
                    });
                }
                consumer.start();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

                LOG.info("LOG startupInfo : {}", startupInfo());

            } catch (Exception ex) {
                LOG.error("DefaultRocketMQConsumerImpl start error!", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private List<RocketMQMessage> convert(List<MessageExt> msgs){
        List<RocketMQMessage> messages = new ArrayList<>();
        for(MessageExt ext : msgs){
            messages.add(new RocketMQMessage(ext));
        }
        return messages;
    }

    public void setMessageListener(MessageListener messageListener) {
        if(this.messageListener != null){
            throw new IllegalArgumentException("messageListener has already set");
        }
        this.messageListener = messageListener;
    }

    public void close() {
        if(start.compareAndSet(true, false)){
            if(this.consumer != null) {
                this.consumer.shutdown();
                LOG.info("DefaultRocketMQConsumerImpl closed !");
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
