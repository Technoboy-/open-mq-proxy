package com.owl.mq.client.util;


import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.client.transport.message.RmqHeader;
import com.owl.mq.client.transport.message.RmqMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class RmqMessageCodec {

    public static ByteBuf encode(RmqMessage message){
        RmqHeader header = message.getHeader();
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        message.setHeaderInBytes(headerInBytes);
        ByteBuf buffer = Unpooled.buffer(4 + 4 + message.getLength());
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(message.getValue().length);
        buffer.writeBytes(message.getValue());
        return buffer;
    }

//    public static ByteBuffer encode(KafkaMessage message){
//        KafkaHeader header = message.getKafkaHeader();
//        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
//        message.setHeaderInBytes(headerInBytes);
//        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + message.getLength());
//        buffer.putInt(headerInBytes.length);
//        buffer.put(headerInBytes);
//        buffer.putInt(message.getKey().length);
//        buffer.put(message.getKey());
//        buffer.putInt(message.getValue().length);
//        buffer.put(message.getValue());
//        return buffer;
//    }

    public static RmqMessage decode(byte[] body){
        ByteBuffer buffer = ByteBuffer.wrap(body);
        return decode(buffer);
    }

    public static RmqMessage decode(ByteBuffer buffer){
        RmqMessage message = new RmqMessage();

        //
        int headerLength = buffer.getInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.get(headerInBytes, 0, headerLength);
        RmqHeader header = (RmqHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, RmqHeader.class);
        message.setHeader(header);
        message.setHeaderInBytes(headerInBytes);

        //
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value, 0, valueLength);
        message.setValue(value);

        return message;
    }

    public static RmqMessage decode(ByteBuf buffer){
        RmqMessage message = new RmqMessage();

        //
        int headerLength = buffer.readInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.readBytes(headerInBytes, 0, headerLength);
        RmqHeader header = (RmqHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, RmqHeader.class);
        message.setHeader(header);
        message.setHeaderInBytes(headerInBytes);
        //
        int valueLength = buffer.readInt();
        byte[] value = new byte[valueLength];
        buffer.readBytes(value, 0, valueLength);
        message.setValue(value);

        return message;
    }

    public static List<RmqMessage> decodes(byte[] body){
        ByteBuffer buffer = ByteBuffer.wrap(body);
        return decodes(buffer);
    }

    public static List<RmqMessage> decodes(ByteBuffer buffer){
        List<RmqMessage> messages = new ArrayList<>();
        //
        while(buffer.hasRemaining()){
            RmqMessage message = new RmqMessage();
            //
            int headerLength = buffer.getInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.get(headerInBytes, 0, headerLength);
            RmqHeader header = (RmqHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, RmqHeader.class);
            message.setHeader(header);
            message.setHeaderInBytes(headerInBytes);
            //
            int valueLength = buffer.getInt();
            byte[] value = new byte[valueLength];
            buffer.get(value, 0, valueLength);
            message.setValue(value);
            //
            messages.add(message);
        }
        return messages;
    }

    public static List<RmqMessage> decodes(ByteBuf buffer){
        List<RmqMessage> messages = new ArrayList<>();
        //
        while(buffer.readableBytes() > 0){
            RmqMessage message = new RmqMessage();
            //
            int headerLength = buffer.readInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.readBytes(headerInBytes, 0, headerLength);
            RmqHeader header = (RmqHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, RmqHeader.class);
            message.setHeader(header);
            message.setHeaderInBytes(headerInBytes);
            //
            int valueLength = buffer.readInt();
            byte[] value = new byte[valueLength];
            buffer.readBytes(value, 0, valueLength);
            message.setValue(value);
            //
            messages.add(message);
        }
        return messages;
    }
}
