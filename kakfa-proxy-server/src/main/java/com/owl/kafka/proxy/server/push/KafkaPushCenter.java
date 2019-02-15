package com.owl.kafka.proxy.server.push;

import com.owl.client.common.serializer.SerializerImpl;
import com.owl.mq.client.service.IdService;
import com.owl.mq.client.transport.message.KafkaHeader;
import com.owl.mq.client.transport.protocol.Command;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.server.push.AbstractPushCenter;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public class KafkaPushCenter extends AbstractPushCenter<ConsumerRecord<byte[], byte[]>> {

    public Packet take() throws InterruptedException{
        Packet packet = retryQueue.peek();
        if(packet != null){
            retryQueue.poll();
        } else{
            ConsumerRecord<byte[], byte[]> record = pushQueue.take();
            packet = new Packet();
            //
            packet.setCmd(Command.PUSH.getCmd());
            packet.setOpaque(IdService.I.getId());

            KafkaHeader kafkaHeader = new KafkaHeader(record.topic(), record.partition(), record.offset(), IdService.I.getId());
            kafkaHeader.setSign(KafkaHeader.Sign.PUSH.getSign());
            byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(kafkaHeader);
            //
            ByteBuf buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + record.key().length + 4 + record.value().length);
            buffer.writeInt(headerInBytes.length);
            buffer.writeBytes(headerInBytes);
            //
            buffer.writeInt(record.key().length);
            buffer.writeBytes(record.key());
            //
            buffer.writeInt(record.value().length);
            buffer.writeBytes(record.value());
            //
            packet.setBody(buffer);
        }
        return packet;
    }

}
