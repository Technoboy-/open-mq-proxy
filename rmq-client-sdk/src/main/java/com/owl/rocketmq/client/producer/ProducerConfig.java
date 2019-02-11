package com.owl.rocketmq.client.producer;

import com.owl.kafka.client.serializer.FastJsonSerializer;
import com.owl.kafka.client.serializer.Serializer;

/**
 * http://kafka.apache.org/0110/documentation.html#producerconfigs
 * @Author: Tboy
 */
public class ProducerConfig{

    public ProducerConfig(String namesrvAddr, String producerGroup){
        this.namesrvAddr = namesrvAddr;
        this.producerGroup = producerGroup;
    }

    /**
     * 服务器列表
     */
    private String namesrvAddr;

    private String producerGroup;

    private int maxMessageSize = 4 * 1024 * 1024; // 4M

    private int retryTimesWhenSendFailed = 2;

    private Serializer serializer;

    public Serializer getSerializer() {
        return serializer;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
