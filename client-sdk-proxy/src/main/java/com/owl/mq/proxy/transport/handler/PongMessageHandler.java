package com.owl.mq.proxy.transport.handler;


import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.protocol.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PongMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PongMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isTraceEnabled()){
            LOGGER.trace("received pong message from : {}", connection.getRemoteAddress());
        }
    }

}