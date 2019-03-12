package com.owl.rocketmq.proxy.server.transport.handler;


import com.owl.client.common.metric.MonitorImpl;
import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.proxy.bo.BrokerNameOffset;
import com.owl.mq.proxy.bo.TopicQueue;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.handler.CommonMessageHandler;
import com.owl.mq.proxy.transport.message.RmqHeader;
import com.owl.mq.proxy.transport.message.RmqMessage;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.RmqMessageCodec;
import com.owl.rocketmq.proxy.server.config.RmqServerConfigs;
import com.owl.rocketmq.proxy.server.consumer.ProxyConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
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

    private volatile ConcurrentMap<TopicQueue, BrokerNameOffset> latestOffsetMap = new ConcurrentHashMap<>();

    private final int interval = RmqServerConfigs.I.getServerCommitOffsetInterval();

    private final int batchSize = RmqServerConfigs.I.getServerCommitOffsetBatchSize();

    private final AtomicLong messageCount = new AtomicLong(1);

    private final ProxyConsumer consumer;

    private final ScheduledExecutorService commitScheduler;

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
        TopicQueue topicPartition = new TopicQueue(rmqHeader.getTopic(), rmqHeader.getQueue());
        BrokerNameOffset brokerNameOffset = latestOffsetMap.get(topicPartition);
        if (brokerNameOffset == null || rmqHeader.getOffset() > brokerNameOffset.getOffset()) {
            latestOffsetMap.put(topicPartition, new BrokerNameOffset(rmqHeader.getBrokerName(), rmqHeader.getOffset()));
        }
    }

    class CommitOffsetTask implements Runnable {

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
                final Map<TopicQueue, BrokerNameOffset> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                for(Map.Entry<TopicQueue, BrokerNameOffset> entry : pre.entrySet()){
                    TopicQueue topicQueue = entry.getKey();
                    BrokerNameOffset brokerNameOffset = entry.getValue();
                    consumer.commit(new MessageQueue(topicQueue.getTopic(), brokerNameOffset.getBrokerName(), topicQueue.getQueue()), brokerNameOffset.getOffset());
                }
            } catch (Throwable ex) {
                LOGGER.error("Commit consumer offset error.", ex);
            } finally {
                MonitorImpl.getDefault().recordCommitCount(1L);
                MonitorImpl.getDefault().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }
}
