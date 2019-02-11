package com.owl.rocketmq.client.producer;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class SendResult implements Serializable {

    private String sendStatus;
    private String topic;
    private String brokerName;
    private int queueId;
    private String msgId;
    private long queueOffset;
    private String transactionId;
    private String offsetMsgId;
    private String regionId;

    public SendResult(org.apache.rocketmq.client.producer.SendResult sendResult){
        this.sendStatus = sendResult.getSendStatus().name();
        this.topic = sendResult.getMessageQueue().getTopic();
        this.brokerName = sendResult.getMessageQueue().getBrokerName();
        this.queueId = sendResult.getMessageQueue().getQueueId();
        this.msgId = sendResult.getMsgId();
        this.queueOffset = sendResult.getQueueOffset();
        this.transactionId = sendResult.getTransactionId();
        this.offsetMsgId = sendResult.getOffsetMsgId();
        this.regionId = sendResult.getRegionId();
    }

    public String getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(String sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getOffsetMsgId() {
        return offsetMsgId;
    }

    public void setOffsetMsgId(String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "sendStatus='" + sendStatus + '\'' +
                ", topic='" + topic + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                ", msgId='" + msgId + '\'' +
                ", queueOffset=" + queueOffset +
                ", transactionId='" + transactionId + '\'' +
                ", offsetMsgId='" + offsetMsgId + '\'' +
                ", regionId='" + regionId + '\'' +
                '}';
    }
}
