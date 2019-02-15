package com.owl.mq.server.transport.handler;

import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.handler.CommonMessageHandler;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.ChannelUtils;
import com.owl.mq.client.util.KafkaPackets;
import com.owl.mq.server.registry.RegistryCenter;
import com.owl.mq.server.service.InstanceHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PingMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received ping : {}, from : {}", packet, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        connection.send(KafkaPackets.pong());
        InstanceHolder.I.get(RegistryCenter.class).getClientRegistry().register(connection);
    }

}
