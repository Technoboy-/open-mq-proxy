package com.owl.rocketmq.proxy.server.config;


import com.owl.client.common.util.*;
import com.owl.mq.proxy.bo.ServerConfigs;

/**
 * @Author: Tboy
 */
public class RmqServerConfigs extends ServerConfigs {

    static final String SERVER_TAGS = "server.tags";

    static final String SERVER_NAMESRV_LIST = "server.namesrv.list";

    static final  String SERVER_CONFIG_FILE = "rmq_server.properties";

    public static RmqServerConfigs I = new RmqServerConfigs(SERVER_CONFIG_FILE);

    public RmqServerConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(StringUtils.isNotEmpty(getServerNamesrvList()), "namesrv should not be empty");

        Preconditions.checkArgument(StringUtils.isNotEmpty(getServerTopic()), "topic should not be empty");
        //
        Preconditions.checkArgument(StringUtils.isNotEmpty(getServerGroupId()), "groupId should not be empty");
    }

    public String getServerTags() {
        return get(SERVER_TAGS);
    }

    public String getServerNamesrvList() {
        return get(SERVER_NAMESRV_LIST);
    }

}
