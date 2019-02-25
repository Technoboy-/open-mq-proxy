package com.owl.rocketmq.proxy.server.consumer;


import com.owl.rocketmq.client.consumer.service.MessageListenerService;
import com.owl.rocketmq.proxy.server.pull.RmqPullCenter;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerService implements MessageListenerService {

    @Override
    public void onMessage(List<MessageExt> msgs) {
        try {
            for(MessageExt msg : msgs){
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
