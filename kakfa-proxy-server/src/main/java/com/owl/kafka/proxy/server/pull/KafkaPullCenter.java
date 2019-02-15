package com.owl.kafka.proxy.server.pull;

import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.client.service.IdService;
import com.owl.mq.client.service.PullStatus;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.server.pull.AbstractPullCenter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public class KafkaPullCenter extends AbstractPullCenter<ConsumerRecord<byte[], byte[]>> {

    public static KafkaPullCenter I = new KafkaPullCenter();

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
            ConsumerRecord<byte[], byte[]> record = pullQueue.poll();
            if(record != null){
                KafkaHeader kafkaHeader = new KafkaHeader(record.topic(), record.partition(), record.offset(),
                        IdService.I.getId(), PullStatus.FOUND.getStatus());
                byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);

                int capacity = 4 + headerInBytes.length + 4 + record.key().length + 4 + record.value().length;
                ByteBuf buffer = bufferPool.allocate(capacity);
                //
                buffer.writeBytes(packet.getBody());
                buffer.writeInt(headerInBytes.length);
                buffer.writeBytes(headerInBytes);
                buffer.writeInt(record.key().length);
                buffer.writeBytes(record.key());
                buffer.writeInt(record.value().length);
                buffer.writeBytes(record.value());

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
