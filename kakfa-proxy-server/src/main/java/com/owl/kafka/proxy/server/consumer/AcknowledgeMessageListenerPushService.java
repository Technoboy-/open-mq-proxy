package com.owl.kafka.proxy.server.consumer;

import com.owl.kafka.client.consumer.service.RebalanceMessageListenerService;
import com.owl.mq.server.push.AbstractPushCenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerPushService<K, V> extends RebalanceMessageListenerService<K, V> {

    private final AbstractPushCenter pushCenter;

    public AcknowledgeMessageListenerPushService(AbstractPushCenter pushCenter){
        this.pushCenter = pushCenter;
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        try {
            pushCenter.putMessage(record);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        //NOP
    }

}
