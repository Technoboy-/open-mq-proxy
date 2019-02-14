package com.owl.kafka.client.proxy.service;

import com.owl.client.proxy.ClientConfigs;
import com.owl.client.proxy.service.PullMessageService;
import com.owl.client.proxy.transport.Address;
import com.owl.client.proxy.transport.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class KafkaPullMessageService extends PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPullMessageService.class);

    private final OffsetStore offsetStore = OffsetStore.I;

    private final int processQueueSize = ClientConfigs.I.getProcessQueueSize();

    public KafkaPullMessageService(NettyClient nettyClient) {
        super(nettyClient);
    }

    @Override
    public boolean checkIfPullImmediately(Address address) {
        LOGGER.warn("flow control, pull later : {} for process queue count : {} , more than config  : {}",
                new Object[]{address, offsetStore.getCount(), processQueueSize});
        return false;
    }
}
