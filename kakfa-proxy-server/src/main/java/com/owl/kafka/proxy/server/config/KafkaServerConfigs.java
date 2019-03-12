package com.owl.kafka.proxy.server.config;


import com.owl.client.common.util.*;
import com.owl.mq.proxy.bo.ServerConfigs;

/**
 * @Author: Tboy
 */
public class KafkaServerConfigs extends ServerConfigs {

    static final String SERVER_KAFKA_SERVER_LIST = "server.kafka.server.list";

    static final  String SERVER_CONFIG_FILE = "kafka_server.properties";

    public static KafkaServerConfigs I = new KafkaServerConfigs(SERVER_CONFIG_FILE);

    public KafkaServerConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(!StringUtils.isEmpty(getServerKafkaServerList()), "kafka server list should not be empty");

        Preconditions.checkArgument(!StringUtils.isEmpty(getServerTopic()), "topic should not be empty");
        //
        Preconditions.checkArgument(!StringUtils.isEmpty(getServerGroupId()), "groupId should not be empty");
    }

    public String getServerKafkaServerList() {
        return get(SERVER_KAFKA_SERVER_LIST);
    }


}
