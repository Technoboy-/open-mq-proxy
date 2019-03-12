package com.owl.mq.proxy.transport.message;

import java.io.Serializable;


/**
 * @Author: Tboy
 */
public class RmqMessage implements Serializable {

    private byte[] headerInBytes;

    private RmqHeader header;

    private byte[] value;

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getLength(){
        return headerInBytes.length + value.length;
    }

    public byte[] getHeaderInBytes() {
        return headerInBytes;
    }

    public void setHeaderInBytes(byte[] headerInBytes) {
        this.headerInBytes = headerInBytes;
    }

    public RmqHeader getHeader() {
        return header;
    }

    public void setHeader(RmqHeader header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "header=" + header +
                "headerLength=" + headerInBytes.length +
                ", valueLength==" + value.length +
                '}';
    }
}
