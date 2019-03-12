package com.owl.mq.proxy.util;


import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.proxy.bo.RegisterContent;
import com.owl.mq.proxy.transport.alloc.ByteBufferPool;
import com.owl.mq.proxy.transport.protocol.Command;
import com.owl.mq.proxy.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: Tboy
 */
public class Packets {

    protected static final ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    protected static ByteBuf EMPTY_BODY = Unpooled.EMPTY_BUFFER;

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

    public static ByteBuf ping(){
        return PING_BUF.duplicate();
    }

    public static Packet register(RegisterContent registerContent){
        Packet register = new Packet();
        register.setOpaque(0);
        register.setCmd(Command.REGISTER.getCmd());

        byte[] body = SerializerImpl.getFastJsonSerializer().serialize(registerContent);
        ByteBuf buffer = bufferPool.allocate(4 + body.length);
        buffer.writeInt(body.length);
        buffer.writeBytes(body);
        register.setBody(buffer);
        return register;
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

    public static Packet pullReq(long opaque){
        Packet pull = new Packet();
        pull.setCmd(Command.PULL_REQ.getCmd());
        pull.setOpaque(opaque);
        pull.setBody(EMPTY_BODY);

        return pull;
    }

}
