package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.config.KafkaClientConfigs;
import com.owl.mq.proxy.service.PullMessageService;
import com.owl.mq.proxy.transport.Address;
import com.owl.mq.proxy.transport.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class KafkaPullMessageService extends PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPullMessageService.class);

    private final KafkaOffsetStore kafkaOffsetStore = KafkaOffsetStore.I;

    private final int processQueueSize = KafkaClientConfigs.I.getProcessQueueSize();

    public KafkaPullMessageService(NettyClient nettyClient) {
        super(nettyClient);
    }

    @Override
    public boolean checkIfPullImmediately(Address address) {
        if(kafkaOffsetStore.getCount() > processQueueSize){
            LOGGER.warn("flow control, pull later : {} for process queue count : {} , more than config  : {}",
                    new Object[]{address, kafkaOffsetStore.getCount(), processQueueSize});
            return false;
        }
        return true;
    }
}
