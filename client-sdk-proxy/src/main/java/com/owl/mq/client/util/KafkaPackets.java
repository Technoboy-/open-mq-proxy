package com.owl.mq.client.util;


import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.client.bo.TopicPartitionOffset;
import com.owl.mq.client.service.IdService;
import com.owl.mq.client.service.PullStatus;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.protocol.Command;
import com.owl.mq.client.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Tboy
 */
public class KafkaPackets extends Packets{

    private static ByteBuf EMPTY_KEY = Unpooled.EMPTY_BUFFER;

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
        buffer.writeBytes(EMPTY_BODY);
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
        buffer.writeBytes(EMPTY_BODY);
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
        buffer.writeBytes(EMPTY_BODY);
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
        ByteBuf buffer = bufferPool.allocate(4 + kafkaMessage.getHeaderInBytes().length + 4 + kafkaMessage.getKey().length + 4 + kafkaMessage.getValue().length);
        buffer.writeInt(kafkaMessage.getHeaderInBytes().length);
        buffer.writeBytes(kafkaMessage.getHeaderInBytes());
        buffer.writeInt(kafkaMessage.getKey().length);
        buffer.writeBytes(kafkaMessage.getKey());
        buffer.writeInt(kafkaMessage.getValue().length);
        buffer.writeBytes(kafkaMessage.getValue());
        //
        back.setBody(buffer);

        return back;
    }

    public static Packet pullNoMsgResp(long opaque){
        Packet packet = new Packet();
        packet.setOpaque(opaque);
        packet.setCmd(Command.PULL_RESP.getCmd());
        //
        KafkaHeader kafkaHeader = new KafkaHeader(PullStatus.NO_NEW_MSG.getStatus());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
        ByteBuf buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_KEY);
        buffer.writeInt(0);
        buffer.writeBytes(EMPTY_BODY);
        //
        packet.setBody(buffer);

        return packet;
    }

}
