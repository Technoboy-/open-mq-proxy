package com.owl.rocketmq.client.proxy.service;

import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.proxy.bo.TopicQueue;
import com.owl.mq.proxy.bo.TopicQueueOffset;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.message.RmqMessage;
import com.owl.mq.proxy.util.RmqPackets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class RmqOffsetStore {

    private static final Logger LOG = LoggerFactory.getLogger(RmqOffsetStore.class);

    public static RmqOffsetStore I = new RmqOffsetStore();

    private final ScheduledExecutorService commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));

    private final RmqOffsetQueue rmqOffsetQueue = new RmqOffsetQueue();

    private volatile ConcurrentMap<TopicQueue, TopicQueueOffset> latestOffsetMap = new ConcurrentHashMap<>();

    private Connection connection;

    public RmqOffsetStore(){
        commitScheduler.scheduleAtFixedRate(new ScheduledCommitOffsetTask(), 5, 5, TimeUnit.MINUTES);
    }

    public void storeOffset(List<RmqMessage> rmqMessages){
        rmqOffsetQueue.put(rmqMessages);
    }

    public void updateOffset(Connection connection, final List<RmqMessage> messages){
        this.connection = connection;
        for(RmqMessage rmqMessage : messages){
            TopicQueueOffset offset = rmqOffsetQueue.remove(rmqMessage.getHeader().getMsgId());
            if(offset != null){
                TopicQueue topicQueue = new TopicQueue(offset.getTopic(), offset.getQueue());
                TopicQueueOffset exist = latestOffsetMap.get(topicQueue);
                if (exist == null || offset.getOffset() > exist.getOffset()) {
                    latestOffsetMap.put(topicQueue, offset);
                }
            }
        }
    }

    class ScheduledCommitOffsetTask implements Runnable {

        @Override
        public void run() {
            try {
                final Map<TopicQueue, TopicQueueOffset> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                for(TopicQueueOffset offset : pre.values()){
                    connection.send(RmqPackets.ackReq(offset));
                }
            } catch (Throwable ex) {
                LOG.error("Commit consumer offset error.", ex);
            }
        }
    }

    public long getCount() {
        return rmqOffsetQueue.getMessageCount();
    }

    public void close(){
        this.commitScheduler.shutdown();
        if (!latestOffsetMap.isEmpty()) {
            new ScheduledCommitOffsetTask().run();
        }
    }
}
