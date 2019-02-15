package com.owl.rocketmq.client.consumer.service;

import com.owl.client.common.metric.MonitorImpl;
import com.owl.client.common.util.CollectionUtils;
import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.service.PullStatus;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.message.RmqHeader;
import com.owl.mq.client.transport.message.RmqMessage;
import com.owl.mq.client.util.RmqPackets;
import com.owl.rocketmq.client.consumer.RocketMQMessage;
import com.owl.rocketmq.client.consumer.listener.ConcurrentMessageListener;
import com.owl.rocketmq.client.consumer.listener.MessageListener;
import com.owl.rocketmq.client.proxy.service.OffsetStore;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class PullMessageListenerService implements MessageListenerService{

    private static final Logger LOG = LoggerFactory.getLogger(PullMessageListenerService.class);

    private final OffsetStore offsetStore = OffsetStore.I;

    private final int parallelism = ClientConfigs.I.getParallelismNum();

    private final int consumeBatchSize = ClientConfigs.I.getConsumeBatchSize();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("PullAcknowledgeMessageListenerService-thread"));

    private final ThreadPoolExecutor consumeExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    private final ConcurrentMessageListener messageListener;

    private Connection connection;

    public PullMessageListenerService(MessageListener messageListener){
        this.messageListener = (ConcurrentMessageListener)messageListener;
    }

    public void onMessage(Connection connection, List<RmqMessage> rmqMessages) {
        this.connection = connection;
        final List<RmqMessage> filteredRmqMessages = filter(rmqMessages);
        if(CollectionUtils.isEmpty(filteredRmqMessages)){
            LOG.debug("no new msg");
            return;
        }
        offsetStore.storeOffset(filteredRmqMessages);
        if(filteredRmqMessages.size() < consumeBatchSize){
            ConsumeRequest consumeRequest = new ConsumeRequest(filteredRmqMessages);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch(RejectedExecutionException ex){
                consumeLater(consumeRequest);
            }
        } else{
            for(int total = 0; total < filteredRmqMessages.size(); ){
                List<RmqMessage> msgList = new ArrayList<>(consumeBatchSize);
                for(int i = 0; i < consumeBatchSize; i++, total++){
                    if(total < filteredRmqMessages.size()){
                        msgList.add(filteredRmqMessages.get(total));
                    } else{
                        break;
                    }
                }
                ConsumeRequest consumeRequest = new ConsumeRequest(msgList);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < filteredRmqMessages.size(); total++) {
                        msgList.add(filteredRmqMessages.get(total));
                    }
                    this.consumeLater(consumeRequest);
                }
            }
        }
    }

    private void consumeLater(ConsumeRequest request){
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                consumeExecutor.submit(request);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void sendBack(RmqMessage rmqMessage){
        try {
            connection.send(RmqPackets.sendBackReq(rmqMessage));
        } catch (ChannelInactiveException e) {
            consumeLater(new ConsumeRequest(Arrays.asList(rmqMessage)));
        }
    }

    class ConsumeRequest implements Runnable{

        private List<RmqMessage> rmqMessages;

        public ConsumeRequest(List<RmqMessage> rmqMessages){
            this.rmqMessages = rmqMessages;
        }

        @Override
        public void run() {
            List<RocketMQMessage<byte[]>> rocketMQMessages = new ArrayList<>(rmqMessages.size());
            for(RmqMessage rmqMessages : rmqMessages){
                long now = System.currentTimeMillis();
                try {
                    RmqHeader rmqHeader = rmqMessages.getHeader();
                    byte[] values = rmqMessages.getValue();
                    RocketMQMessage<byte[]> rocketMQMessage = new RocketMQMessage<>();
                    rocketMQMessages.add(rocketMQMessage);
                } catch (Throwable ex) {
                    MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
                    LOG.error("onMessage error", ex);
                    sendBack(rmqMessages);
                } finally {
                    MonitorImpl.getDefault().recordConsumeProcessCount(1);
                    MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
                }
            }
            messageListener.onMessage(rocketMQMessages);
        }
    }

    private List<RmqMessage> filter(List<RmqMessage> rmqMessages){
        List<RmqMessage> newRmqMessages = new ArrayList<>(rmqMessages.size());
        for(RmqMessage rmqMessage : rmqMessages){
            PullStatus pullStatus = PullStatus.of(rmqMessage.getHeader().getPullStatus());
            if(PullStatus.FOUND == pullStatus){
                newRmqMessages.add(rmqMessage);
            }
        }
        return newRmqMessages;
    }


    @Override
    public void onMessage(List<MessageExt> msgs) {
        throw new UnsupportedOperationException("unsupport method");
    }

    @Override
    public void close() {

    }
}
