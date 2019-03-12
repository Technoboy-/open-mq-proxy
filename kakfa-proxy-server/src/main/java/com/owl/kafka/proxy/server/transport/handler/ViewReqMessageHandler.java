package com.owl.kafka.proxy.server.transport.handler;

import com.owl.client.common.serializer.SerializerImpl;
import com.owl.kafka.proxy.server.service.DLQService;
import com.owl.mq.proxy.transport.Connection;
import com.owl.mq.proxy.transport.alloc.ByteBufferPool;
import com.owl.mq.proxy.transport.handler.CommonMessageHandler;
import com.owl.mq.proxy.transport.message.KafkaHeader;
import com.owl.mq.proxy.transport.message.KafkaMessage;
import com.owl.mq.proxy.transport.protocol.Command;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.MessageCodec;
import com.owl.mq.proxy.util.KafkaPackets;
import com.owl.kafka.client.consumer.Record;

import com.owl.mq.server.service.InstanceHolder;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class ViewReqMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewReqMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received view kafkaMessage : {}", packet);
        KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
        KafkaHeader kafkaHeader = kafkaMessage.getHeader();
        Record<byte[], byte[]> record = InstanceHolder.I.get(DLQService.class).view(kafkaHeader.getMsgId());
        if(record != null){
            connection.send(viewResp(packet.getOpaque(), kafkaHeader.getMsgId(), record));
        } else{
            connection.send(KafkaPackets.noViewMsgResp(packet.getOpaque()));
        }
    }

    private Packet viewResp(long opaque, long msgId, Record<byte[], byte[]> record){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        //
        KafkaHeader kafkaHeader = new KafkaHeader(record.getTopic(), record.getPartition(), record.getOffset(), msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);

        ByteBuf buffer = ByteBufferPool.DEFAULT.allocate(4 + headerInBytes.length + 4 + record.getKey().length + 4 + record.getValue().length);
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(record.getKey().length);
        buffer.writeBytes(record.getKey());
        buffer.writeInt(record.getValue().length);
        buffer.writeBytes(record.getValue());
        //
        viewResp.setBody(buffer);

        return viewResp;
    }
}
