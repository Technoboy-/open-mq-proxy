package com.owl.rocketmq.client.proxy.transport.handler;

import com.owl.mq.proxy.service.InvokerPromise;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.handler.CommonMessageHandler;
import com.owl.mq.proxy.transport.message.RmqMessage;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.ChannelUtils;
import com.owl.mq.proxy.util.RmqMessageCodec;
import com.owl.rocketmq.client.proxy.service.MessageListenerService;
import com.owl.rocketmq.client.proxy.service.PullMessageListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: Tboy
 */
public class PullRespMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullRespMessageHandler.class);

    private final PullMessageListenerService messageListenerService;

    public PullRespMessageHandler(MessageListenerService messageListenerService){
        this.messageListenerService = (PullMessageListenerService)messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received pull message: {}, from : {}", packet, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        InvokerPromise invokerPromise = InvokerPromise.get(packet.getOpaque());
        if(invokerPromise != null){
            InvokerPromise.receive(packet);
            if(invokerPromise.getInvokeCallback() != null){
                invokerPromise.executeInvokeCallback();
            }
        }
        List<RmqMessage> messages = RmqMessageCodec.decodes(packet.getBody());
        this.messageListenerService.onMessage(connection, messages);
    }


}
