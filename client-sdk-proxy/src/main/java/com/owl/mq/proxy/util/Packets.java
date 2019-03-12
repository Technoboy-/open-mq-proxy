package com.owl.mq.proxy.util;


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

    //TODO 需要注册信息。将客户端监听的topic等信息传给服务器.
    public static Packet register(String topic){
        Packet register = new Packet();
        register.setOpaque(0);
        register.setCmd(Command.UNREGISTER.getCmd());
        register.setBody(EMPTY_BODY);
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
