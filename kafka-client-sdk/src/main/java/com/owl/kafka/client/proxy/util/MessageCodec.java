package com.owl.kafka.client.proxy.util;

import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.serializer.SerializerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class MessageCodec {

    public static ByteBuf encode(Message message){
        Header header = message.getHeader();
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        message.setHeaderInBytes(headerInBytes);
        ByteBuf buffer = Unpooled.buffer(4 + 4 + 4 + message.getLength());
        buffer.writeInt(headerInBytes.length);
        buffer.writeBytes(headerInBytes);
        buffer.writeInt(message.getKey().length);
        buffer.writeBytes(message.getKey());
        buffer.writeInt(message.getValue().length);
        buffer.writeBytes(message.getValue());
        return buffer;
    }

//    public static ByteBuffer encode(Message message){
//        Header header = message.getHeader();
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

    public static Message decode(byte[] body){
        ByteBuffer buffer = ByteBuffer.wrap(body);
        return decode(buffer);
    }

    public static Message decode(ByteBuffer buffer){
        Message message = new Message();

        //
        int headerLength = buffer.getInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.get(headerInBytes, 0, headerLength);
        Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, Header.class);
        message.setHeader(header);
        message.setHeaderInBytes(headerInBytes);
        //
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key, 0, keyLength);
        message.setKey(key);
        //
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value, 0, valueLength);
        message.setValue(value);

        return message;
    }

    public static Message decode(ByteBuf buffer){
        Message message = new Message();

        //
        int headerLength = buffer.readInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.readBytes(headerInBytes, 0, headerLength);
        Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, Header.class);
        message.setHeader(header);
        message.setHeaderInBytes(headerInBytes);
        //
        int keyLength = buffer.readInt();
        byte[] key = new byte[keyLength];
        buffer.readBytes(key, 0, keyLength);
        message.setKey(key);
        //
        int valueLength = buffer.readInt();
        byte[] value = new byte[valueLength];
        buffer.readBytes(value, 0, valueLength);
        message.setValue(value);

        return message;
    }

    public static List<Message> decodes(byte[] body){
        ByteBuffer buffer = ByteBuffer.wrap(body);
        return decodes(buffer);
    }

    public static List<Message> decodes(ByteBuffer buffer){
        List<Message> messages = new ArrayList<>();
        //
        while(buffer.hasRemaining()){
            Message message = new Message();
            //
            int headerLength = buffer.getInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.get(headerInBytes, 0, headerLength);
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, Header.class);
            message.setHeader(header);
            message.setHeaderInBytes(headerInBytes);
            //
            int keyLength = buffer.getInt();
            byte[] key = new byte[keyLength];
            buffer.get(key, 0, keyLength);
            message.setKey(key);
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

    public static List<Message> decodes(ByteBuf buffer){
        List<Message> messages = new ArrayList<>();
        //
        while(buffer.readableBytes() > 0){
            Message message = new Message();
            //
            int headerLength = buffer.readInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.readBytes(headerInBytes, 0, headerLength);
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, Header.class);
            message.setHeader(header);
            message.setHeaderInBytes(headerInBytes);
            //
            int keyLength = buffer.readInt();
            byte[] key = new byte[keyLength];
            buffer.readBytes(key, 0, keyLength);
            message.setKey(key);
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
