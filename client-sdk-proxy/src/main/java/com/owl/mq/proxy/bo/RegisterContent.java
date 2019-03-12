package com.owl.mq.proxy.bo;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RegisterContent implements Serializable {

    private String topic;

    private String groupId;

    public RegisterContent(){
        //
    }

    public RegisterContent( String topic, String groupId){
        this.topic = topic;
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
}
