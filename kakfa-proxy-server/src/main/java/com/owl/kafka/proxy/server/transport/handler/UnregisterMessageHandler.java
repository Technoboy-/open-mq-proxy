package com.owl.kafka.proxy.server.transport.handler;

import com.owl.kafka.proxy.server.registry.RegistryCenter;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.handler.CommonMessageHandler;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.ChannelUtils;
import com.owl.mq.server.service.InstanceHolder;
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
        InstanceHolder.I.get(RegistryCenter.class).getClientRegistry().unregister(connection);
    }
}
