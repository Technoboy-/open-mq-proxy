package com.owl.kafka.client.consumer.service;

import com.owl.client.common.metric.MonitorImpl;
import com.owl.client.common.util.CollectionUtils;
import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.service.PullStatus;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.util.KafkaPackets;
import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.consumer.listener.AcknowledgeMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;

import com.owl.kafka.client.proxy.service.OffsetStore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class PullAcknowledgeMessageListenerService<K, V> implements MessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(PullAcknowledgeMessageListenerService.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("PullAcknowledgeMessageListenerService-thread"));

    private final int parallelism = ClientConfigs.I.getParallelismNum();

    private final int consumeBatchSize = ClientConfigs.I.getConsumeBatchSize();

    private final ThreadPoolExecutor consumeExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AcknowledgeMessageListener<K, V> messageListener;

    private Connection connection;

    private final OffsetStore offsetStore = OffsetStore.I;

    public PullAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener) {
        this.consumer = consumer;
        this.messageListener = (AcknowledgeMessageListener)messageListener;
        MonitorImpl.getDefault().recordConsumeHandlerCount(1);
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        //
    }

    public void onMessage(Connection connection, List<KafkaMessage> kafkaMessages) {
        this.connection = connection;
        final List<KafkaMessage> filteredKafkaMessages = filter(kafkaMessages);
        if(CollectionUtils.isEmpty(filteredKafkaMessages)){
            LOG.debug("no new msg");
            return;
        }
        offsetStore.storeOffset(filteredKafkaMessages);
        if(filteredKafkaMessages.size() < consumeBatchSize){
            ConsumeRequest consumeRequest = new ConsumeRequest(filteredKafkaMessages);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch(RejectedExecutionException ex){
                consumeLater(consumeRequest);
            }
        } else{
            for(int total = 0; total < filteredKafkaMessages.size(); ){
                List<KafkaMessage> msgList = new ArrayList<>(consumeBatchSize);
                for(int i = 0; i < consumeBatchSize; i++, total++){
                    if(total < filteredKafkaMessages.size()){
                        msgList.add(filteredKafkaMessages.get(total));
                    } else{
                        break;
                    }
                }
                ConsumeRequest consumeRequest = new ConsumeRequest(msgList);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < filteredKafkaMessages.size(); total++) {
                        msgList.add(filteredKafkaMessages.get(total));
                    }
                    this.consumeLater(consumeRequest);
                }
            }
        }
    }

    private List<KafkaMessage> filter(List<KafkaMessage> kafkaMessages){
        List<KafkaMessage> newKafkaMessages = new ArrayList<>(kafkaMessages.size());
        for(KafkaMessage kafkaMessage : kafkaMessages){
            PullStatus pullStatus = PullStatus.of(kafkaMessage.getHeader().getPullStatus());
            if(PullStatus.FOUND == pullStatus){
                newKafkaMessages.add(kafkaMessage);
            }
        }
        return newKafkaMessages;
    }

    private void consumeLater(ConsumeRequest request){
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                consumeExecutor.submit(request);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void sendBack(KafkaMessage kafkaMessage){
        try {
            connection.send(KafkaPackets.sendBackReq(kafkaMessage));
        } catch (ChannelInactiveException e) {
            consumeLater(new ConsumeRequest(Arrays.asList(kafkaMessage)));
        }
    }

    class ConsumeRequest implements Runnable{

        private List<KafkaMessage> kafkaMessages;

        public ConsumeRequest(List<KafkaMessage> kafkaMessages){
            this.kafkaMessages = kafkaMessages;
        }

        @Override
        public void run() {
            for(KafkaMessage kafkaMessage : kafkaMessages){
                long now = System.currentTimeMillis();
                try {
                    KafkaHeader kafkaHeader = kafkaMessage.getHeader();
                    ConsumerRecord record = new ConsumerRecord(kafkaHeader.getTopic(), kafkaHeader.getPartition(), kafkaHeader.getOffset(), kafkaMessage.getKey(), kafkaMessage.getValue());
                    final Record<K, V> r = consumer.toRecord(record);
                    r.setMsgId(kafkaHeader.getMsgId());
                    messageListener.onMessage(r, new AcknowledgeMessageListener.Acknowledgment() {
                        @Override
                        public void acknowledge() {
                            processConsumeResult(r);
                        }
                    });
                } catch (Throwable ex) {
                    MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
                    LOG.error("onMessage error", ex);
                    sendBack(kafkaMessage);
                } finally {
                    MonitorImpl.getDefault().recordConsumeProcessCount(1);
                    MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
                }
            }
        }
    }

    private void processConsumeResult(final Record<K, V> record){
        offsetStore.updateOffset(connection, record.getMsgId());
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
