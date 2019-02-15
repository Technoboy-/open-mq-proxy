package com.owl.rocketmq.client.proxy.service;

import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.message.RmqMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author: Tboy
 */
public class OffsetStore {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetStore.class);

    public static OffsetStore I = new OffsetStore();

    protected final ScheduledExecutorService commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));

    private final OffsetQueue offsetQueue = new OffsetQueue();

    private Connection connection;

    public void storeOffset(List<RmqMessage> rmqMessages){
        offsetQueue.put(rmqMessages);
    }

    public void updateOffset(Connection connection, long msgId){
        this.connection = connection;

    }

    class ScheduledCommitOffsetTask implements Runnable {

        @Override
        public void run() {
            try {
            } catch (Throwable ex) {
                LOG.error("Commit consumer offset error.", ex);
            }
        }
    }

    public long getCount() {
        return offsetQueue.getMessageCount();
    }


    public void close(){
        this.commitScheduler.shutdown();
    }
}
