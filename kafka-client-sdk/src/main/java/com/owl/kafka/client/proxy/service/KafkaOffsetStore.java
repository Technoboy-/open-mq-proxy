package com.owl.kafka.client.proxy.service;

import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.proxy.bo.TopicPartitionOffset;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.message.KafkaMessage;
import com.owl.mq.proxy.util.KafkaPackets;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class KafkaOffsetStore {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetStore.class);

    public static KafkaOffsetStore I = new KafkaOffsetStore();

    private final ScheduledExecutorService commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));

    private final KafkaOffsetQueue kafkaOffsetQueue = new KafkaOffsetQueue();

    private volatile ConcurrentMap<TopicPartition, TopicPartitionOffset> latestOffsetMap = new ConcurrentHashMap<>();

    private Connection connection;

    public KafkaOffsetStore(){
        commitScheduler.scheduleAtFixedRate(new ScheduledCommitOffsetTask(), 5, 5, TimeUnit.MINUTES);
    }

    public void storeOffset(List<KafkaMessage> kafkaMessages){
        kafkaOffsetQueue.put(kafkaMessages);
    }

    public void updateOffset(Connection connection, long msgId){
        this.connection = connection;
        TopicPartitionOffset offset = kafkaOffsetQueue.remove(msgId);
        if(offset != null){
            TopicPartition topicPartition = new TopicPartition(offset.getTopic(), offset.getPartition());
            TopicPartitionOffset exist = latestOffsetMap.get(topicPartition);
            if (exist == null || offset.getOffset() > exist.getOffset()) {
                latestOffsetMap.put(topicPartition, offset);
            }
        }
    }

    class ScheduledCommitOffsetTask implements Runnable {

        @Override
        public void run() {
            try {
                final Map<TopicPartition, TopicPartitionOffset> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                for(TopicPartitionOffset offset : pre.values()){
                    connection.send(KafkaPackets.ackPullReq(offset));
                }
            } catch (Throwable ex) {
                LOG.error("Commit consumer offset error.", ex);
            }
        }
    }

    public long getCount() {
        return kafkaOffsetQueue.getMessageCount();
    }

    public void close(){
        this.commitScheduler.shutdown();
        if (!latestOffsetMap.isEmpty()) {
            new ScheduledCommitOffsetTask().run();
        }
    }
}
