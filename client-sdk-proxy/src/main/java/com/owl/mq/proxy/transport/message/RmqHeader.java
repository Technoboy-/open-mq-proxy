package com.owl.mq.proxy.transport.message;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RmqHeader implements Serializable {

    private String brokerName;

    private String topic;

    private String tags;

    private int queue;

    private long offset;

    private String msgId;

    private byte repost;

    private byte pullStatus;

    public RmqHeader() {
    }

    public RmqHeader(byte pullStatus) {
        this.pullStatus = pullStatus;
    }

    public RmqHeader(String msgId) {
        this.msgId = msgId;
    }

    public RmqHeader(String brokerName, String topic, int queue, long offset, String msgId) {
        this.brokerName = brokerName;
        this.topic = topic;
        this.queue = queue;
        this.offset = offset;
        this.msgId = msgId;
        this.repost = (byte)1;
    }

    public RmqHeader(String brokerName, String topic, String tags, int queue, long offset, String msgId, byte pullStatus) {
        this.brokerName = brokerName;
        this.topic = topic;
        this.tags = tags;
        this.queue = queue;
        this.offset = offset;
        this.msgId = msgId;
        this.repost = (byte)1;
        this.pullStatus = pullStatus;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getMsgId() {
        return msgId;
    }

    public byte getPullStatus() {
        return pullStatus;
    }

    public void setPullStatus(byte pullStatus) {
        this.pullStatus = pullStatus;
    }

    public void setRepost(byte repost) {
        this.repost = repost;
    }

    public byte getRepost() {
        return repost;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueue() {
        return queue;
    }

    public void setQueue(int queue) {
        this.queue = queue;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "KafkaHeader{" +
                "topic='" + topic + '\'' +
                ", queue=" + queue +
                ", offset=" + offset +
                ", msgId=" + msgId +
                ", repost=" + repost +
                ", pullStatus=" + pullStatus +
                '}';
    }
}
