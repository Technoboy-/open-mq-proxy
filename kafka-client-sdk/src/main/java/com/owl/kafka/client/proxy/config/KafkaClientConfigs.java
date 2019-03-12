package com.owl.kafka.client.proxy.config;


import com.owl.client.common.util.Preconditions;
import com.owl.client.common.util.StringUtils;
import com.owl.mq.proxy.bo.ClientConfigs;

/**
 * @Author: Tboy
 */
public class KafkaClientConfigs extends ClientConfigs {

    static final  String CLIENT_CONFIG_FILE = "kafka_client.properties";

    public static final KafkaClientConfigs I = new KafkaClientConfigs(CLIENT_CONFIG_FILE);

    private KafkaClientConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(StringUtils.isNotEmpty(getTopic()), "topic should not be empty");
    }

}
