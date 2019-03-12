package com.owl.mq.proxy.transport.handler;

import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.ChannelUtils;
import com.owl.mq.proxy.util.Packets;
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
        connection.send(Packets.pong());
    }

}
