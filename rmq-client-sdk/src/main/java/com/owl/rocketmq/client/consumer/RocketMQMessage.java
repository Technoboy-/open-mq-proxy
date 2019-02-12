package com.owl.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageExt;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RocketMQMessage<V> implements Serializable {

    private static final long serialVersionUID = 1L;

    private String mqId;

    private String topic;

    private String tags;

    private V body;

    public RocketMQMessage(MessageExt ext) {
        this.mqId = ext.getMsgId();
        this.topic = ext.getTopic();
        this.tags = ext.getTags();
    }


    public String getMqId() {
        return mqId;
    }


    public void setMqId(String mqId) {
        this.mqId = mqId;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public V getBody() {
        return body;
    }

    public void setBody(V body) {
        this.body = body;
    }

    public String toString() {
        return "RocketMQMessage [mqId=" + mqId + ", topic=" + topic + ", tags=" + tags + ", body="
                + body + "]";
    }
}
