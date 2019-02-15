package com.owl.rocketmq.client.proxy.service;

import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.service.PullMessageService;
import com.owl.mq.client.transport.Address;
import com.owl.mq.client.transport.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class RmqPullMessageService extends PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RmqPullMessageService.class);

    public static OffsetStore offsetStore = new OffsetStore();

    private final int processQueueSize = ClientConfigs.I.getProcessQueueSize();

    public RmqPullMessageService(NettyClient nettyClient) {
        super(nettyClient);
    }

    @Override
    public boolean checkIfPullImmediately(Address address) {
        if(offsetStore.getCount() > processQueueSize){
            LOGGER.warn("flow control, pull later : {} for process queue count : {} , more than config  : {}",
                    new Object[]{address, offsetStore.getCount(), processQueueSize});
            return false;
        }
        return true;
    }
}
