package com.owl.mq.proxy.transport.handler;

import com.owl.mq.proxy.registry.RegistryManager;
import com.owl.mq.proxy.service.InstanceHolder;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.ChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class UnregisterMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnregisterMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received unregister message : {}, from : {}", packet, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        InstanceHolder.I.get(RegistryManager.class).getClientRegistry().unregister(connection);
    }
}
