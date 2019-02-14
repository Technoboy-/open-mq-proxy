package com.owl.kafka.proxy.server.transport.handler;

import com.owl.kafka.proxy.server.pull.KafkaPullCenter;
import com.owl.kafka.proxy.server.push.service.DLQService;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.handler.CommonMessageHandler;
import com.owl.mq.client.transport.message.Header;
import com.owl.mq.client.transport.message.Message;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.ChannelUtils;
import com.owl.mq.client.util.MessageCodec;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.service.InstanceHolder;
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
        Message message = MessageCodec.decode(packet.getBody());
        Header header = message.getHeader();
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received sendback message : {}, from : {}", header, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        if(header.getRepost() >= repostCount){
            InstanceHolder.I.get(DLQService.class).write(header.getMsgId(), packet);
        } else{
            header.setRepost((byte)(header.getRepost() + 1));
            ByteBuf buffer = MessageCodec.encode(message);
            packet.setBody(buffer);
            KafkaPullCenter.I.reputMessage(packet);
        }

    }
}
