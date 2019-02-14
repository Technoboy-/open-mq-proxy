package com.owl.kafka.client.proxy.transport.handler;


import com.owl.client.proxy.service.InvokerPromise;
import com.owl.client.proxy.transport.Connection;
import com.owl.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.client.proxy.transport.protocol.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class ViewMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received view msg : {}", packet);
        InvokerPromise.receive(packet);
    }


}
