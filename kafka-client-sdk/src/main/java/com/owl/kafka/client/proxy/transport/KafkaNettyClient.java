package com.owl.kafka.client.proxy.transport;

import com.owl.client.common.util.Constants;
import com.owl.client.proxy.transport.NettyClient;
import com.owl.client.proxy.transport.handler.MessageDispatcher;
import com.owl.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.proxy.transport.handler.*;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @Author: Tboy
 */
public class KafkaNettyClient extends NettyClient {

    private final MessageListenerService messageListenerService;

    public KafkaNettyClient(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    public void initHandler(MessageDispatcher dispatcher){
        dispatcher.register(Command.PONG, new PongMessageHandler());
        dispatcher.register(Command.VIEW_RESP, new ViewMessageHandler());
        //
        String proxyModel = System.getProperty(Constants.PROXY_MODEL);
        if(ConsumerConfig.ProxyModel.PULL == ConsumerConfig.ProxyModel.PULL.valueOf(proxyModel)){
            dispatcher.register(Command.PULL_RESP, new PullRespMessageHandler(messageListenerService));
        } else{
            dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        }
    }

}
