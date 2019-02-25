package com.owl.rocketmq.proxy.server.transport.handler;

import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.handler.CommonMessageHandler;
import com.owl.mq.client.transport.message.RmqHeader;
import com.owl.mq.client.transport.message.RmqMessage;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.ChannelUtils;
import com.owl.mq.client.util.RmqMessageCodec;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.rocketmq.proxy.server.pull.RmqPullCenter;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class SendBackMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendBackMessageHandler.class);

    private final int repostCount = ServerConfigs.I.getServerRepostCount();

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        RmqMessage rmqMessage = RmqMessageCodec.decode(packet.getBody());
        RmqHeader rmqHeader = rmqMessage.getHeader();
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received sendback rmqMessage : {}, from : {}", rmqHeader, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        if(rmqHeader.getRepost() >= repostCount){
            //TODO
        } else{
            rmqHeader.setRepost((byte)(rmqHeader.getRepost() + 1));
            ByteBuf buffer = RmqMessageCodec.encode(rmqMessage);
            packet.setBody(buffer);
            RmqPullCenter.I.reputMessage(packet);
        }

    }
}
