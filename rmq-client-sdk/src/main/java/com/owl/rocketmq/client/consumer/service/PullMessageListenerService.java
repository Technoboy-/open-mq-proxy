package com.owl.rocketmq.client.consumer.service;

import com.owl.client.common.metric.MonitorImpl;
import com.owl.client.common.util.CollectionUtils;
import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.service.PullStatus;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.transport.message.RmqMessage;
import com.owl.mq.client.util.RmqPackets;
import com.owl.rocketmq.client.consumer.RocketMQMessage;
import com.owl.rocketmq.client.consumer.listener.ConcurrentMessageListener;
import com.owl.rocketmq.client.consumer.listener.MessageListener;
import com.owl.rocketmq.client.proxy.service.RmqOffsetStore;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class PullMessageListenerService implements MessageListenerService{

    private static final Logger LOG = LoggerFactory.getLogger(PullMessageListenerService.class);

    private final RmqOffsetStore rmqOffsetStore = RmqOffsetStore.I;

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
        rmqOffsetStore.storeOffset(filteredRmqMessages);
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

    private void sendBack(List<RmqMessage> messages){
        try {
            for(RmqMessage rmqMessage : messages){
                connection.send(RmqPackets.sendBackReq(rmqMessage));
            }
        } catch (ChannelInactiveException e) {
            consumeLater(new ConsumeRequest(messages));
        }
    }

    class ConsumeRequest implements Runnable{

        private List<RmqMessage> rmqMessages;

        public ConsumeRequest(List<RmqMessage> rmqMessages){
            this.rmqMessages = rmqMessages;
        }

        @Override
        public void run() {
            List<RocketMQMessage<byte[]>> messages = convert(this.rmqMessages);
            long now = System.currentTimeMillis();
            try {
                messageListener.onMessage(messages);
                processConsumeResult(rmqMessages);
            } catch (Throwable ex){
                MonitorImpl.getDefault().recordConsumeProcessErrorCount(messages.size());
                LOG.error("onMessage error", ex);
                sendBack(rmqMessages);
            } finally {
                MonitorImpl.getDefault().recordConsumeProcessCount(messages.size());
                MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
            }
        }
    }

    private void processConsumeResult(final List<RmqMessage> messages){
        rmqOffsetStore.updateOffset(connection, messages);
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

    private List<RocketMQMessage<byte[]>> convert(List<RmqMessage> rmqMessages){
        List<RocketMQMessage<byte[]>> rocketMQMessages = new ArrayList<>(rmqMessages.size());
        for(RmqMessage rmqMessage : rmqMessages){
            RocketMQMessage<byte[]> rocketMQMessage = new RocketMQMessage<>();
            rocketMQMessage.setTopic(rmqMessage.getHeader().getTopic());
            rocketMQMessage.setTags(rmqMessage.getHeader().getTags());
            rocketMQMessage.setMqId(rmqMessage.getHeader().getMsgId());
            rocketMQMessage.setBody(rmqMessage.getValue());
            rocketMQMessages.add(rocketMQMessage);
        }
        return rocketMQMessages;
    }


    @Override
    public void onMessage(MessageQueue messageQueue, List<MessageExt> msgs) {
        throw new UnsupportedOperationException("unsupport method");
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
        consumeExecutor.shutdown();
    }
}
