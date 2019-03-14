package com.owl.kafka.client.producer;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class SendResult<K, V> implements Serializable {

    private String topic;

    private Integer partition;

    private long offset;

    private long timestamp;

    private K key;

    private V value;

    public SendResult(){
        //NOP
    }

    public SendResult(Integer partition, long offset){
        this.partition = partition;
        this.offset = offset;
    }

    public SendResult(String topic, Integer partition, long offset,long timestamp, K key, V value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
