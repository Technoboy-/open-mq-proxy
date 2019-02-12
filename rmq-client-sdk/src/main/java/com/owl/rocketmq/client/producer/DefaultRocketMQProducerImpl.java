package com.owl.rocketmq.client.producer;

import com.owl.kafka.client.serializer.Serializer;
import com.owl.rocketmq.client.util.Preconditions;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class DefaultRocketMQProducerImpl<V> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRocketMQProducerImpl.class);

    private ProducerConfig producerConfig;

    private final DefaultMQProducer producer;

    private final Serializer serializer;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public DefaultRocketMQProducerImpl(ProducerConfig producerConfig){
        this.producerConfig = producerConfig;
        this.producer = new DefaultMQProducer(producerConfig.getProducerGroup());
        this.producer.setMaxMessageSize(producerConfig.getMaxMessageSize());
        this.producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        this.producer.setNamesrvAddr(producerConfig.getNamesrvAddr());
        this.serializer = producerConfig.getSerializer();
    }

    public void start() {

        Preconditions.checkArgument(serializer != null, "serializer should not be null");

        if(start.compareAndSet(false, true)){
            try {
                producer.start();
                LOG.info("DefaultRocketMQProducerImpl started! namesrvAddr : {}, producerGroup : {}", producerConfig.getNamesrvAddr(), producerConfig.getProducerGroup());
            } catch (Exception ex) {
                LOG.error("DefaultRocketMQProducerImpl start error.", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    public SendResult sendSync(String topic, String tags, V value) {
        SendResult ret = null;
        try {
            byte[] body = serializer.serialize(value);
            Message message = new Message(topic, tags, body);
            org.apache.rocketmq.client.producer.SendResult result = producer.send(message);
            if (LOG.isDebugEnabled()) {
                LOG.debug("send msg result : {}", result);
            }
            ret = new SendResult(result);
        } catch (Exception e) {
            LOG.error("send error, topic : {}, tags : {}, value : {} error : {}", topic, tags, value, e);
            throw new RuntimeException(e);
        }
        return ret;
    }

    public void sendAsync(String topic, String tags, V value, Callback callback) {
        try {
            byte[] body = serializer.serialize(value);
            Message message = new Message(topic, tags, body);
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(org.apache.rocketmq.client.producer.SendResult sendResult) {
                    if(sendResult != null && callback != null){
                        callback.onSuccess(new SendResult(sendResult));
                    }
                }

                @Override
                public void onException(Throwable e) {
                    if(callback != null){
                        callback.onException(e);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("send error, topic : {}, tags : {}, value : {} error : {}", topic, tags, value, e);
            callback.onException(e);
        }
    }

    public void close() {
        if(start.compareAndSet(true, false)){
            if(this.producer != null) {
                this.producer.shutdown();
                LOG.info("DefaultRocketMQProducerImpl closed !");
            }
        }
    }
}
