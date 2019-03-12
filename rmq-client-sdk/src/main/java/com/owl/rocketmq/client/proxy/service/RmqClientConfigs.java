package com.owl.rocketmq.client.proxy.service;


import com.owl.client.common.util.ClassPathPropertyLoader;
import com.owl.client.common.util.Constants;
import com.owl.client.common.util.Preconditions;
import com.owl.client.common.util.StringUtils;

/**
 * @Author: Tboy
 */
public class RmqClientConfigs extends ClassPathPropertyLoader {

    static final String CLIENT_TOPIC = "client.topic";

    static final String CLIENT_WORKER_NUM = "proxy.worker.num";

    static final String CLIENT_PARALLELISM_NUM = "proxy.parallelism.num";

    static final String CLIENT_PROCESS_QUEUE_SIZE = "proxy.process.queue.size";

    static final String CLIENT_CONSUME_BATCH_SIZE = "proxy.consume.batch.size";

    static final  String CLIENT_CONFIG_FILE = "rmq_client.properties";

    public static final RmqClientConfigs I = new RmqClientConfigs(CLIENT_CONFIG_FILE);

    private RmqClientConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(!StringUtils.isEmpty(getTopic()), "topic should not be empty");
    }

    public String getTopic(){
        return get(CLIENT_TOPIC);
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
