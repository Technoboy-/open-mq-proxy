package com.owl.kafka.proxy.server.transport.handler;

import com.owl.kafka.proxy.server.config.KafkaServerConfigs;
import com.owl.kafka.proxy.server.pull.KafkaPullCenter;
import com.owl.kafka.proxy.server.service.DLQService;
import com.owl.mq.proxy.service.InstanceHolder;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.handler.CommonMessageHandler;
import com.owl.mq.proxy.transport.message.KafkaHeader;
import com.owl.mq.proxy.transport.message.KafkaMessage;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.ChannelUtils;
import com.owl.mq.proxy.util.MessageCodec;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class SendBackMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendBackMessageHandler.class);

    private final int repostCount = KafkaServerConfigs.I.getServerRepostCount();

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
        KafkaHeader kafkaHeader = kafkaMessage.getHeader();
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received sendback kafkaMessage : {}, from : {}", kafkaHeader, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        if(kafkaHeader.getRepost() >= repostCount){
            InstanceHolder.I.get(DLQService.class).write(kafkaHeader.getMsgId(), packet);
        } else{
            kafkaHeader.setRepost((byte)(kafkaHeader.getRepost() + 1));
            ByteBuf buffer = MessageCodec.encode(kafkaMessage);
            packet.setBody(buffer);
            KafkaPullCenter.I.reputMessage(packet);
        }

    }
}
