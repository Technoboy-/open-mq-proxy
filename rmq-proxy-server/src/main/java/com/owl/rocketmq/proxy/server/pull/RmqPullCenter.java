package com.owl.rocketmq.proxy.server.pull;

import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.proxy.service.PullStatus;
import com.owl.mq.proxy.transport.message.RmqHeader;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.RmqPackets;
import com.owl.mq.proxy.pull.AbstractPullCenter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @Author: Tboy
 */
public class RmqPullCenter extends AbstractPullCenter<MessageExt> {

    public static RmqPullCenter I = new RmqPullCenter();

    @Override
    public Packet pullNoMsgResp(long opaque) {
        return RmqPackets.pullNoMsgResp(opaque);
    }

    public boolean poll(Packet packet) {
        boolean polled = false;
        Packet one = retryQueue.peek();
        if(one != null){
            retryQueue.poll();
            CompositeByteBuf compositeByteBuf = bufferPool.compositeBuffer();
            compositeByteBuf.addComponent(true, packet.getBody());
            compositeByteBuf.addComponent(true, one.getBody());
            packet.setBody(compositeByteBuf);
            polled = true;
        } else{
            MessageExt record = pullQueue.poll();
            if(record != null){
                String brokerName = record.getProperty("brokerName");
                RmqHeader rmqHeader = new RmqHeader(brokerName, record.getTopic(), record.getTags(), record.getQueueId(), record.getQueueOffset(),
                        record.getMsgId(), PullStatus.FOUND.getStatus());
                byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(rmqHeader);

                int capacity = 4 + headerInBytes.length + 4 + record.getBody().length;
                ByteBuf buffer = bufferPool.allocate(capacity);
                //
                buffer.writeInt(headerInBytes.length);
                buffer.writeBytes(headerInBytes);
                buffer.writeInt(record.getBody().length);
                buffer.writeBytes(record.getBody());

                //
                CompositeByteBuf compositeByteBuf = bufferPool.compositeBuffer();
                compositeByteBuf.addComponent(true, packet.getBody());
                compositeByteBuf.addComponent(true, buffer);
                //
                packet.setBody(compositeByteBuf);
                polled = true;
            }
        }
        return polled;
    }

}
