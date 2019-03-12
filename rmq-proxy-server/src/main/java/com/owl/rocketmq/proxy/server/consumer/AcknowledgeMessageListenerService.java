package com.owl.rocketmq.proxy.server.consumer;


import com.owl.rocketmq.client.proxy.service.MessageListenerService;
import com.owl.rocketmq.proxy.server.pull.RmqPullCenter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerService implements MessageListenerService {

    @Override
    public void onMessage(MessageQueue messageQueue, List<MessageExt> msgs) {
        try {
            for(MessageExt msg : msgs){
                msg.getProperties().put("brokerName", messageQueue.getBrokerName());
                RmqPullCenter.I.putMessage(msg);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        RmqPullCenter.I.close();
    }

}
