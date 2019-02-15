package com.owl.mq.client.util;


import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.message.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class MessageCodec {

    public static ByteBuf encode(KafkaMessage kafkaMessage){
        KafkaHeader kafkaHeader = kafkaMessage.getHeader();
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
        kafkaMessage.setHeaderInBytes(headerInBytes);
        ByteBuf buffer = Unpooled.buffer(4 + 4 + 4 + kafkaMessage.getLength());
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(kafkaMessage.getKey().length);
        buffer.writeBytes(kafkaMessage.getKey());
        buffer.writeInt(kafkaMessage.getValue().length);
        buffer.writeBytes(kafkaMessage.getValue());
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

    public static KafkaMessage decode(byte[] body){
        ByteBuffer buffer = ByteBuffer.wrap(body);
        return decode(buffer);
    }

    public static KafkaMessage decode(ByteBuffer buffer){
        KafkaMessage kafkaMessage = new KafkaMessage();

        //
        int headerLength = buffer.getInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.get(headerInBytes, 0, headerLength);
        KafkaHeader kafkaHeader = (KafkaHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, KafkaHeader.class);
        kafkaMessage.setHeader(kafkaHeader);
        kafkaMessage.setHeaderInBytes(headerInBytes);
        //
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key, 0, keyLength);
        kafkaMessage.setKey(key);
        //
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value, 0, valueLength);
        kafkaMessage.setValue(value);

        return kafkaMessage;
    }

    public static KafkaMessage decode(ByteBuf buffer){
        KafkaMessage kafkaMessage = new KafkaMessage();

        //
        int headerLength = buffer.readInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.readBytes(headerInBytes, 0, headerLength);
        KafkaHeader kafkaHeader = (KafkaHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, KafkaHeader.class);
        kafkaMessage.setHeader(kafkaHeader);
        kafkaMessage.setHeaderInBytes(headerInBytes);
        //
        int keyLength = buffer.readInt();
        byte[] key = new byte[keyLength];
        buffer.readBytes(key, 0, keyLength);
        kafkaMessage.setKey(key);
        //
        int valueLength = buffer.readInt();
        byte[] value = new byte[valueLength];
        buffer.readBytes(value, 0, valueLength);
        kafkaMessage.setValue(value);

        return kafkaMessage;
    }

    public static List<KafkaMessage> decodes(byte[] body){
        ByteBuffer buffer = ByteBuffer.wrap(body);
        return decodes(buffer);
    }

    public static List<KafkaMessage> decodes(ByteBuffer buffer){
        List<KafkaMessage> kafkaMessages = new ArrayList<>();
        //
        while(buffer.hasRemaining()){
            KafkaMessage kafkaMessage = new KafkaMessage();
            //
            int headerLength = buffer.getInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.get(headerInBytes, 0, headerLength);
            KafkaHeader kafkaHeader = (KafkaHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, KafkaHeader.class);
            kafkaMessage.setHeader(kafkaHeader);
            kafkaMessage.setHeaderInBytes(headerInBytes);
            //
            int keyLength = buffer.getInt();
            byte[] key = new byte[keyLength];
            buffer.get(key, 0, keyLength);
            kafkaMessage.setKey(key);
            //
            int valueLength = buffer.getInt();
            byte[] value = new byte[valueLength];
            buffer.get(value, 0, valueLength);
            kafkaMessage.setValue(value);
            //
            kafkaMessages.add(kafkaMessage);
        }
        return kafkaMessages;
    }

    public static List<KafkaMessage> decodes(ByteBuf buffer){
        List<KafkaMessage> kafkaMessages = new ArrayList<>();
        //
        while(buffer.readableBytes() > 0){
            KafkaMessage kafkaMessage = new KafkaMessage();
            //
            int headerLength = buffer.readInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.readBytes(headerInBytes, 0, headerLength);
            KafkaHeader kafkaHeader = (KafkaHeader) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, KafkaHeader.class);
            kafkaMessage.setHeader(kafkaHeader);
            kafkaMessage.setHeaderInBytes(headerInBytes);
            //
            int keyLength = buffer.readInt();
            byte[] key = new byte[keyLength];
            buffer.readBytes(key, 0, keyLength);
            kafkaMessage.setKey(key);
            //
            int valueLength = buffer.readInt();
            byte[] value = new byte[valueLength];
            buffer.readBytes(value, 0, valueLength);
            kafkaMessage.setValue(value);
            //
            kafkaMessages.add(kafkaMessage);
        }
        return kafkaMessages;
    }
}
