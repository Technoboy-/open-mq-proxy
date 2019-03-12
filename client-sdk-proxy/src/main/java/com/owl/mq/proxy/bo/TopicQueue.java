package com.owl.mq.proxy.bo;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class TopicQueue implements Serializable{

    private final String topic;
    private final int queue;

    public TopicQueue(String topic, int queue){
        this.topic = topic;
        this.queue = queue;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueue() {
        return queue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicQueue that = (TopicQueue) o;
        return getQueue() == that.getQueue() &&
                Objects.equals(getTopic(), that.getTopic());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTopic(), getQueue());
    }

    @Override
    public String toString() {
        return "TopicQueue{" +
                "topic='" + topic + '\'' +
                ", queue=" + queue +
                '}';
    }

}


