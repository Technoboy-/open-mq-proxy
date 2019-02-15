package com.owl.rocketmq.client.proxy.transport;

import com.owl.mq.client.transport.NettyClient;
import com.owl.mq.client.transport.handler.MessageDispatcher;
import com.owl.mq.client.transport.handler.PongMessageHandler;
import com.owl.mq.client.transport.handler.ViewMessageHandler;
import com.owl.mq.client.transport.protocol.Command;
import com.owl.rocketmq.client.consumer.service.MessageListenerService;
import com.owl.rocketmq.client.proxy.transport.handler.PullRespMessageHandler;

/**
 * @Author: Tboy
 */
public class RmqNettyClient extends NettyClient {

    private final MessageListenerService messageListenerService;

    public RmqNettyClient(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    public void initHandler(MessageDispatcher dispatcher){
        dispatcher.register(Command.PONG, new PongMessageHandler());
        dispatcher.register(Command.VIEW_RESP, new ViewMessageHandler());
        //
        dispatcher.register(Command.PULL_RESP, new PullRespMessageHandler(messageListenerService));
    }

}
