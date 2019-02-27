package com.owl.rocketmq.proxy.server.consumer;


import com.owl.client.common.serializer.Serializer;
import com.owl.client.common.util.Preconditions;
import com.owl.rocketmq.client.consumer.ConsumerConfig;
import com.owl.rocketmq.client.consumer.RocketMQMessage;
import com.owl.rocketmq.client.consumer.service.MessageListenerService;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class ProxyConsumer<V> {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConsumer.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final DefaultMQPushConsumer consumer;
    private final Serializer serializer;
    private final ConsumerConfig consumerConfig;
    private MessageListenerService messageListenerService;

    public ProxyConsumer(ConsumerConfig consumerConfig){
        this.consumerConfig = consumerConfig;
        this.consumer = new DefaultMQPushConsumer(consumerConfig.getConsumerGroup());
        this.consumer.setMessageModel(consumerConfig.getMessageModel());
        this.consumer.setNamesrvAddr(consumerConfig.getNamesrvAddr());
        this.consumer.setConsumeFromWhere(consumerConfig.getConsumeFromWhere());
        this.consumer.setConsumeMessageBatchMaxSize(consumerConfig.getConsumeMessageBatchMaxSize());
        try {
            this.consumer.subscribe(this.consumerConfig.getTopic(), this.consumerConfig.getTags());
        } catch (Exception ex){
            LOG.error("ProxyConsumer subscribe error!", ex);
            throw new RuntimeException(ex);
        }
        this.serializer = consumerConfig.getSerializer();
    }


    public void start() {

        Preconditions.checkArgument(messageListenerService != null, "messageListenerService should not be null");
        Preconditions.checkArgument(serializer != null, "serializer should not be null");

        if(start.compareAndSet(false, true)){
            try {
                consumer.registerMessageListener(new MessageListenerConcurrently() {
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                    ConsumeConcurrentlyContext context) {
                        try {
                            messageListenerService.onMessage(context.getMessageQueue(), msgs);
                        } catch (Throwable ex){
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                });
                //turn off auto commmit offset in RMQ client
                consumer.setPersistConsumerOffsetInterval(-1);
                consumer.start();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

                LOG.info("LOG startupInfo : {}", startupInfo());

            } catch (Exception ex) {
                LOG.error("ProxyConsumer start error!", ex);
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

    public void commit(MessageQueue messageQueue, long offset){
        this.consumer.getDefaultMQPushConsumerImpl().updateConsumeOffset(messageQueue, offset);
        try {
            this.consumer.getDefaultMQPushConsumerImpl().getOffsetStore().updateConsumeOffsetToBroker(messageQueue, offset, true);
        } catch (RemotingException | MQBrokerException | MQClientException | InterruptedException ex)  {
            //LOG
        }
    }

    public void setMessageListenerService(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    public void close() {
        if(start.compareAndSet(true, false)){
            if(this.consumer != null) {
                this.consumer.shutdown();
                LOG.info("ProxyConsumer closed !");
            }
        }
    }

    private String startupInfo(){
        StringBuilder builder = new StringBuilder(100);
        builder.append("ProxyConsumer started! ");
        builder.append("  namesrvAddr : ").append(consumerConfig.getNamesrvAddr());
        builder.append("  topic: ").append(consumerConfig.getTopic());
        builder.append("  tags : ").append(consumerConfig.getTags());
        builder.append("  consumerGroup : ").append(consumerConfig.getConsumerGroup());
        return builder.toString();
    }
}
