package com.owl.rocketmq.client.proxy.transport;

import com.owl.mq.proxy.transport.NettyClient;
import com.owl.mq.proxy.transport.handler.MessageDispatcher;
import com.owl.mq.proxy.transport.handler.PongMessageHandler;
import com.owl.mq.proxy.transport.handler.ViewMessageHandler;
import com.owl.mq.proxy.transport.protocol.Command;
import com.owl.rocketmq.client.proxy.service.MessageListenerService;
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
