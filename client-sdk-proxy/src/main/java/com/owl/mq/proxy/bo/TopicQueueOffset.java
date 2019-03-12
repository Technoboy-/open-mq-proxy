package com.owl.mq.proxy.bo;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class TopicQueueOffset implements Serializable, Comparable<TopicQueueOffset> {

    private final String brokerName;
    private final String topic;
    private final int queue;
    private final long offset;
    private final String msgId;

    public TopicQueueOffset(String brokerName, String topic, int queue, long offset, String msgId){
        this.brokerName = brokerName;
        this.topic = topic;
        this.queue = queue;
        this.offset = offset;
        this.msgId = msgId;
    }


    public String getBrokerName() {
        return brokerName;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueue() {
        return queue;
    }

    public long getOffset() {
        return offset;
    }

    public String getMsgId() {
        return msgId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicQueueOffset that = (TopicQueueOffset) o;
        return getQueue() == that.getQueue() &&
                getOffset() == that.getOffset() &&
                Objects.equals(getBrokerName(), that.getBrokerName()) &&
                Objects.equals(getTopic(), that.getTopic()) &&
                Objects.equals(getMsgId(), that.getMsgId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBrokerName(), getTopic(), getQueue(), getOffset(), getMsgId());
    }

    @Override
    public String toString() {
        return "TopicQueueOffset{" +
                "brokerName='" + brokerName + '\'' +
                ", topic='" + topic + '\'' +
                ", queue=" + queue +
                ", offset=" + offset +
                ", msgId='" + msgId + '\'' +
                '}';
    }

    @Override
    public int compareTo(TopicQueueOffset o) {
        return this.getQueue() > o.getQueue() ? 1 : (this.getQueue() < o.getQueue() ? -1 : (this.offset > o.offset ? 1 : this.offset < o.offset ? -1 : 0) );
    }
}


