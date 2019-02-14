package com.owl.rocketmq.client.consumer;

import com.owl.client.common.serializer.Serializer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @Author: Tboy
 */
public class ConsumerConfig{

    /**
     * subscribe模式 指定namesrvAddr
     * @param namesrvAddr
     * @param topic
     * @param consumerGroup
     */
    public ConsumerConfig(String namesrvAddr, String topic, String tags, String consumerGroup) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.tags = tags;
        this.consumerGroup = consumerGroup;
    }

    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * namesrv服务器列表
     */
    private String namesrvAddr;


    /**
     * 消费主题
     */
    private String topic;

    /**
     * 消费消息的tags
     */
    private String tags;


    /**
     * 消费组
     */
    private String consumerGroup;

    /**
     * 消费点
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * 每次消费消息的数据
     */
    private int consumeMessageBatchMaxSize = 1;

    private int pullBatchSize = 32;

    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 64;

    private Serializer serializer;

    public Serializer getSerializer() {
        return serializer;
    }

    private boolean useProxy = false;

    public boolean isUseProxy() {
        return useProxy;
    }

    public void setUseProxy(boolean useProxy) {
        this.useProxy = useProxy;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }
}
