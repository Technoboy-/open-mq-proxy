package com.owl.mq.proxy.bo;


import com.owl.client.common.util.*;

/**
 * @Author: Tboy
 */
public abstract class ClientConfigs extends ClassPathPropertyLoader {

    static final String CLIENT_TOPIC = "client.topic";

    static final String CLIENT_GROUP_ID = "client.groupId";

    static final String CLIENT_WORKER_NUM = "client.worker.num";

    static final String CLIENT_PARALLELISM_NUM = "client.parallelism.num";

    static final String CLIENT_PROCESS_QUEUE_SIZE = "client.process.queue.size";

    static final String CLIENT_CONSUME_BATCH_SIZE = "client.consume.batch.size";

    public String getZookeeperServerList(){
        return get(ZookeeperConstants.ZOOKEEPER_SERVER_LIST);
    }

    public ClientConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(StringUtils.isNotEmpty(getTopic()), "topic should not be empty");
    }

    public String getTopic(){
        return get(CLIENT_TOPIC);
    }

    public String getGroupId(){
        return get(CLIENT_GROUP_ID);
    }

    public int getWorkerNum(){
        return getInt(CLIENT_WORKER_NUM, Constants.CPU_SIZE);
    }

    public int getParallelismNum(){
        return getInt(CLIENT_PARALLELISM_NUM, 1);
    }

    public int getConsumeBatchSize(){
        return getInt(CLIENT_CONSUME_BATCH_SIZE, 2);
    }

    public int getProcessQueueSize(){
        return getInt(CLIENT_PROCESS_QUEUE_SIZE, 1000);
    }

}
