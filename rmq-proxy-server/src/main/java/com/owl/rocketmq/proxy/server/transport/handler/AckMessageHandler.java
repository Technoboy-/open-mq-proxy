package com.owl.rocketmq.proxy.server.transport.handler;


import com.owl.client.common.metric.MonitorImpl;
import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.handler.CommonMessageHandler;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.message.RmqHeader;
import com.owl.mq.client.transport.message.RmqMessage;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.MessageCodec;
import com.owl.mq.client.util.RmqMessageCodec;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.push.service.MessageHolder;
import com.owl.rocketmq.proxy.server.consumer.ProxyConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class AckMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckMessageHandler.class);

//    private volatile ConcurrentMap<TopicPartition, OffsetAndMetadata> latestOffsetMap = new ConcurrentHashMap<>();

    private final AtomicLong messageCount = new AtomicLong(1);

    private final ProxyConsumer consumer;

    private final ScheduledExecutorService commitScheduler;

    private final int interval = ServerConfigs.I.getServerCommitOffsetInterval();

    private final int batchSize = ServerConfigs.I.getServerCommitOffsetBatchSize();

    public AckMessageHandler(ProxyConsumer consumer){
        this.consumer = consumer;
        this.commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));
        this.commitScheduler.scheduleAtFixedRate(new CommitOffsetTask(), interval, interval, TimeUnit.SECONDS);
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        RmqMessage rmqMessage = RmqMessageCodec.decode(packet.getBody());
        LOGGER.debug("received pull ack msg : {}", rmqMessage);
        acknowledge(rmqMessage.getHeader());
    }

    protected void acknowledge(RmqHeader rmqHeader){
        if (messageCount.incrementAndGet() % batchSize == 0) {
            commitScheduler.execute(new CommitOffsetTask());
        }
        toOffsetMap(rmqHeader);
    }

    private void toOffsetMap(RmqHeader rmqHeader){
//        TopicPartition topicPartition = new TopicPartition(kafkaHeader.getTopic(), kafkaHeader.getPartition());
//        OffsetAndMetadata offsetAndMetadata = latestOffsetMap.get(topicPartition);
//        if (offsetAndMetadata == null || kafkaHeader.getOffset() > offsetAndMetadata.offset()) {
//            latestOffsetMap.put(topicPartition, new OffsetAndMetadata(kafkaHeader.getOffset()));
//        }
    }

    class CommitOffsetTask implements Runnable {

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
//                final Map<TopicPartition, OffsetAndMetadata> pre = latestOffsetMap;
//                latestOffsetMap = new ConcurrentHashMap<>();
//                if (pre.isEmpty()) {
//                    return;
//                }
//                consumer
            } catch (Throwable ex) {
                LOGGER.error("Commit consumer offset error.", ex);
            } finally {
                MonitorImpl.getDefault().recordCommitCount(1L);
                MonitorImpl.getDefault().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }
}
