package com.owl.mq.proxy.bo;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class BrokerNameOffset implements Serializable {

    private final String brokerName;

    private final long offset;

    public BrokerNameOffset(String brokerName, long offset){
        this.brokerName = brokerName;
        this.offset = offset;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "BrokerNameOffset{" +
                "brokerName='" + brokerName + '\'' +
                ", offset=" + offset +
                '}';
    }
}
