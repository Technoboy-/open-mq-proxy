package com.owl.kafka.proxy.server.transport.handler;


import com.owl.client.common.metric.MonitorImpl;
import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.handler.CommonMessageHandler;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.MessageCodec;

import com.owl.kafka.proxy.server.consumer.ProxyConsumer;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.push.service.MessageHolder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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

    private volatile ConcurrentMap<TopicPartition, OffsetAndMetadata> latestOffsetMap = new ConcurrentHashMap<>();

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
        KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
        KafkaHeader.Sign sign = KafkaHeader.Sign.of(kafkaMessage.getHeader().getSign());
        if(sign == null){
            LOGGER.error("sign is empty, opaque : {}, kafkaMessage : {}", packet.getOpaque(), kafkaMessage);
            return;
        }
        switch (sign){
            case PUSH:
                LOGGER.debug("received push ack msg : {}", kafkaMessage);
                acknowledge(kafkaMessage.getHeader());
                boolean result = MessageHolder.fastRemove(kafkaMessage);
                if(!result){
                    LOGGER.warn("MessageHolder not found ack opaque : {}, just ignore", packet.getOpaque());
                }
                break;
            case PULL:
                LOGGER.debug("received pull ack msg : {}", kafkaMessage);
                acknowledge(kafkaMessage.getHeader());
                break;

        }
    }

    protected void acknowledge(KafkaHeader kafkaHeader){
        if (messageCount.incrementAndGet() % batchSize == 0) {
            commitScheduler.execute(new CommitOffsetTask());
        }
        toOffsetMap(kafkaHeader);
    }

    private void toOffsetMap(KafkaHeader kafkaHeader){
        TopicPartition topicPartition = new TopicPartition(kafkaHeader.getTopic(), kafkaHeader.getPartition());
        OffsetAndMetadata offsetAndMetadata = latestOffsetMap.get(topicPartition);
        if (offsetAndMetadata == null || kafkaHeader.getOffset() > offsetAndMetadata.offset()) {
            latestOffsetMap.put(topicPartition, new OffsetAndMetadata(kafkaHeader.getOffset()));
        }
    }

    class CommitOffsetTask implements Runnable {

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
                final Map<TopicPartition, OffsetAndMetadata> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                consumer.commit(pre);
            } catch (Throwable ex) {
                LOGGER.error("Commit consumer offset error.", ex);
            } finally {
                MonitorImpl.getDefault().recordCommitCount(1L);
                MonitorImpl.getDefault().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }
}
