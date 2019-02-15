package com.owl.mq.client.util;


import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.client.service.IdService;
import com.owl.mq.client.service.PullStatus;
import com.owl.mq.client.service.TopicPartitionOffset;
import com.owl.mq.client.transport.alloc.ByteBufferPool;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.protocol.Command;
import com.owl.mq.client.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Tboy
 */
public class KafkaPackets {

    private static final ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    private static ByteBuf EMPTY_BODY = Unpooled.EMPTY_BUFFER;

    private static ByteBuf EMPTY_KEY = Unpooled.EMPTY_BUFFER;

    private static ByteBuf EMPTY_VALUE = Unpooled.EMPTY_BUFFER;

    private static final ByteBuf PING_BUF;

    static {
        ByteBuf ping = Unpooled.buffer();
        ping.writeByte(Packet.MAGIC);
        ping.writeByte(Packet.VERSION);
        ping.writeByte(Command.PING.getCmd());
        ping.writeLong(0);
        ping.writeInt(0);
        ping.writeBytes(EMPTY_BODY);
        PING_BUF = Unpooled.unreleasableBuffer(ping).asReadOnly();

    }

    public static ByteBuf pingContent(){
        return PING_BUF.duplicate();
    }

    public static ByteBuf registerContent(){
        return PING_BUF.duplicate();
    }

    public static Packet pong(){
        Packet pong = new Packet();
        pong.setOpaque(0);
        pong.setCmd(Command.PONG.getCmd());
        pong.setBody(EMPTY_BODY);
        return pong;
    }

    public static Packet unregister(){
        Packet unregister = new Packet();
        unregister.setOpaque(0);
        unregister.setCmd(Command.UNREGISTER.getCmd());
        unregister.setBody(EMPTY_BODY);
        return unregister;
    }

    public static Packet ackPushReq(long msgId){
        Packet packet = new Packet();
        packet.setCmd(Command.ACK.getCmd());
        packet.setOpaque(IdService.I.getId());

        KafkaHeader kafkaHeader = new KafkaHeader(msgId);
        kafkaHeader.setSign(KafkaHeader.Sign.PUSH.getSign());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
        //
        ByteBuf buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_KEY);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

    public static Packet ackPullReq(TopicPartitionOffset topicPartitionOffset){
        Packet packet = new Packet();
        packet.setCmd(Command.ACK.getCmd());
        packet.setOpaque(IdService.I.getId());

        KafkaHeader kafkaHeader = new KafkaHeader(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition(), topicPartitionOffset.getOffset(), topicPartitionOffset.getMsgId());
        kafkaHeader.setSign(KafkaHeader.Sign.PULL.getSign());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
        //
        ByteBuf buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_KEY);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

    public static Packet viewReq(long msgId){
        Packet packet = new Packet();
        packet.setCmd(Command.VIEW_REQ.getCmd());
        packet.setOpaque(IdService.I.getId());
        //
        KafkaHeader kafkaHeader = new KafkaHeader(msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
        //
        ByteBuf buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_KEY);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

    public static Packet noViewMsgResp(long opaque){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        viewResp.setBody(EMPTY_BODY);

        return viewResp;
    }

    public static Packet sendBackReq(KafkaMessage kafkaMessage){
        Packet back = new Packet();
        back.setCmd(Command.SEND_BACK.getCmd());
        back.setOpaque(IdService.I.getId());
        //
        ByteBuf buffer = bufferPool.allocate(kafkaMessage.getHeaderInBytes().length + 4 + 4 + 4);
        buffer.writeInt(kafkaMessage.getHeaderInBytes().length);
        buffer.writeBytes(kafkaMessage.getHeaderInBytes());
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_KEY);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_VALUE);
        //
        back.setBody(buffer);

        return back;
    }

    public static Packet pullReq(long opaque){
        Packet pull = new Packet();
        pull.setCmd(Command.PULL_REQ.getCmd());
        pull.setOpaque(opaque);
        pull.setBody(EMPTY_BODY);

        return pull;
    }

    public static Packet pullNoMsgResp(long opaque){
        Packet packet = new Packet();
        packet.setOpaque(opaque);
        packet.setCmd(Command.PULL_RESP.getCmd());
        //
        KafkaHeader kafkaHeader = new KafkaHeader(PullStatus.NO_NEW_MSG.getStatus());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
        ByteBuf buffer = bufferPool.allocate(headerInBytes.length + 4 + 4 + 4);
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_KEY);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

}
