package com.owl.kafka.proxy.server.transport.handler;

import com.owl.client.common.serializer.SerializerImpl;
import com.owl.client.proxy.transport.Connection;
import com.owl.client.proxy.transport.alloc.ByteBufferPool;
import com.owl.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.client.proxy.transport.message.Header;
import com.owl.client.proxy.transport.message.Message;
import com.owl.client.proxy.transport.protocol.Command;
import com.owl.client.proxy.transport.protocol.Packet;
import com.owl.client.proxy.util.MessageCodec;
import com.owl.client.proxy.util.Packets;
import com.owl.kafka.client.consumer.Record;

import com.owl.kafka.proxy.server.biz.service.InstanceHolder;
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
        LOGGER.debug("received view message : {}", packet);
        Message message = MessageCodec.decode(packet.getBody());
        Header header = message.getHeader();
        Record<byte[], byte[]> record = InstanceHolder.I.getDLQService().view(header.getMsgId());
        if(record != null){
            connection.send(viewResp(packet.getOpaque(), header.getMsgId(), record));
        } else{
            connection.send(Packets.noViewMsgResp(packet.getOpaque()));
        }
    }

    private Packet viewResp(long opaque, long msgId, Record<byte[], byte[]> record){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        //
        Header header = new Header(record.getTopic(), record.getPartition(), record.getOffset(), msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);

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
