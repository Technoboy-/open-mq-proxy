package com.owl.kafka.client.proxy.transport;

import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.proxy.config.KafkaClientConfigs;
import com.owl.kafka.client.proxy.transport.handler.PullRespMessageHandler;
import com.owl.kafka.client.proxy.transport.handler.ViewMessageHandler;
import com.owl.mq.proxy.transport.NettyClient;
import com.owl.mq.proxy.transport.handler.MessageDispatcher;
import com.owl.mq.proxy.transport.handler.PongMessageHandler;
import com.owl.mq.proxy.transport.protocol.Command;

/**
 * @Author: Tboy
 */
public class KafkaNettyClient extends NettyClient {

    private final MessageListenerService messageListenerService;

    public KafkaNettyClient(MessageListenerService messageListenerService){
        super(KafkaClientConfigs.I);
        this.messageListenerService = messageListenerService;
    }

    public void initHandler(MessageDispatcher dispatcher){
        dispatcher.register(Command.PONG, new PongMessageHandler());
        dispatcher.register(Command.VIEW_RESP, new ViewMessageHandler());
        //
        dispatcher.register(Command.PULL_RESP, new PullRespMessageHandler(messageListenerService));

    }

}
