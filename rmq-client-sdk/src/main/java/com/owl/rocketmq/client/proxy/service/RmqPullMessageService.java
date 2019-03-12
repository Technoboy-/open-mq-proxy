package com.owl.rocketmq.client.proxy.service;

import com.owl.mq.proxy.service.PullMessageService;
import com.owl.mq.proxy.transport.Address;
import com.owl.mq.proxy.transport.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class RmqPullMessageService extends PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RmqPullMessageService.class);

    public static RmqOffsetStore rmqOffsetStore = new RmqOffsetStore();

    private final int processQueueSize = RmqClientConfigs.I.getProcessQueueSize();

    public RmqPullMessageService(NettyClient nettyClient) {
        super(nettyClient);
    }

    @Override
    public boolean checkIfPullImmediately(Address address) {
        if(rmqOffsetStore.getCount() > processQueueSize){
            LOGGER.warn("flow control, pull later : {} for process queue count : {} , more than config  : {}",
                    new Object[]{address, rmqOffsetStore.getCount(), processQueueSize});
            return false;
        }
        return true;
    }
}
