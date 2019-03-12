package com.owl.mq.proxy.transport.handler;


import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    void handle(Connection connection, Packet packet) throws Exception;

    void beforeHandle(Connection connection, Packet packet) throws Exception;

    void afterHandle(Connection connection, Packet packet) throws Exception;

}
