package com.owl.mq.proxy.transport.message;

import java.io.Serializable;


/**
 * @Author: Tboy
 */
public class KafkaMessage implements Serializable {

    private byte[] headerInBytes;

    private KafkaHeader header;

    private byte[] key;

    private byte[] value;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getLength(){
        return headerInBytes.length + key.length + value.length;
    }

    public byte[] getHeaderInBytes() {
        return headerInBytes;
    }

    public void setHeaderInBytes(byte[] headerInBytes) {
        this.headerInBytes = headerInBytes;
    }

    public void setHeader(KafkaHeader header) {
        this.header = header;
    }

    public KafkaHeader getHeader() {
        return header;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "header=" + header +
                "headerLength=" + headerInBytes.length +
                ", keyLength==" + key.length +
                ", valueLength==" + value.length +
                '}';
    }
}
