package com.owl.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageExt;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RocketMQMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String mqId;

    private String topic;

    private String tags;

    private byte[] body;

    public RocketMQMessage(MessageExt ext) {
        this.mqId = ext.getMsgId();
        this.topic = ext.getTopic();
        this.tags = ext.getTags();
        this.body = ext.getBody();
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

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String toString() {
        return "RocketMQMessage [mqId=" + mqId + ", topic=" + topic + ", tags=" + tags + ", body="
                + body + "]";
    }
}
