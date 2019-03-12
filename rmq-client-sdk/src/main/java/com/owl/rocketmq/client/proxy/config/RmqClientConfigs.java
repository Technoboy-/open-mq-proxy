package com.owl.rocketmq.client.proxy.config;


import com.owl.client.common.util.Preconditions;
import com.owl.client.common.util.StringUtils;
import com.owl.mq.proxy.bo.ClientConfigs;

/**
 * @Author: Tboy
 */
public class RmqClientConfigs extends ClientConfigs {

    static final  String CLIENT_CONFIG_FILE = "rmq_client.properties";

    public static final RmqClientConfigs I = new RmqClientConfigs(CLIENT_CONFIG_FILE);

    private RmqClientConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(!StringUtils.isEmpty(getTopic()), "topic should not be empty");
    }

}
