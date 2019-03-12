package com.owl.kafka.proxy.server.service;

import com.owl.client.common.util.Preconditions;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.proxy.server.consumer.DLQConsumer;
import com.owl.kafka.proxy.server.consumer.ProxyConsumer;
import com.owl.kafka.proxy.server.config.KafkaServerConfigs;
import com.owl.mq.proxy.service.InstanceHolder;
import com.owl.mq.proxy.transport.message.KafkaMessage;
import com.owl.mq.proxy.transport.protocol.Packet;
import com.owl.mq.proxy.util.MessageCodec;
import com.owl.mq.proxy.zookeeper.ZookeeperClient;
import com.owl.mq.proxy.bo.ResendPacket;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class DLQService {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConsumer.class);

    private static final String DLQ_DATA_PATH = "/%s";  //-dlq/msgId

    private DLQConsumer dlqConsumer;

    private final Producer<byte[], byte[]> producer;

    private final String topic;

    public DLQService(String bootstrapServers, String topic, String groupId){
        //config for consumer
        this.topic = topic + "-dlq";

        //config for producer
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put("bootstrap.servers", bootstrapServers);
        producerConfigs.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfigs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(producerConfigs);

        this.dlqConsumer = new DLQConsumer(bootstrapServers, this.topic, groupId);
        InstanceHolder.I.set(this);
    }

    public void close(){
        this.producer.close();
        this.dlqConsumer.close();
    }

    public Record<byte[], byte[]> view(long msgId){
        try {
            String dlp = String.format(this.topic + DLQ_DATA_PATH, msgId);
            byte[] data = InstanceHolder.I.get(ZookeeperClient.class).getData(dlp);
            ByteBuffer wrap = ByteBuffer.wrap(data);
            long offset = wrap.getLong();
            ConsumerRecord<byte[], byte[]> record = dlqConsumer.seek(offset);
            if(record != null){
                Record<byte[], byte[]> r = new Record<>(msgId, record.topic(), 0, offset, record.key(), record.value(), record.timestamp());
                return r;
            }
        } catch (Exception ex){
            LOG.error("view error", ex);
        }
        return null;
    }

    public void write(ResendPacket resendPacket){
        Preconditions.checkArgument(resendPacket.getRepost() >= KafkaServerConfigs.I.getServerMessageRepostTimes(), "resendPacket must repost more than " + KafkaServerConfigs.I.getServerMessageRepostTimes() + " times");
        try {
            Packet packet = resendPacket.getPacket();
            KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
            String dlp = String.format(this.topic + DLQ_DATA_PATH, resendPacket.getMsgId());
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(this.topic, 0, kafkaMessage.getKey(), kafkaMessage.getValue());
            this.producer.send(record, new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        try {
                            InstanceHolder.I.get(ZookeeperClient.class).createPersistent(dlp, toByteArray(metadata.offset()));
                        } catch (Exception ex) {
                            LOG.error("write to zk path : {}, data : {}, error : {}", new Object[]{dlp, metadata.offset(), ex});
                        }
                    } else {
                        LOG.error("write to kafka error", exception);
                    }
                }
            });
        } catch (Exception ex){
            LOG.error("write error", ex);
        }
    }

    public void write(long msgId, Packet packet){
        try {
            String dlp = String.format(this.topic + DLQ_DATA_PATH, msgId);
            KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(this.topic, 0, kafkaMessage.getKey(), kafkaMessage.getValue());
            this.producer.send(record, new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        try {
                            InstanceHolder.I.get(ZookeeperClient.class).createPersistent(dlp, toByteArray(metadata.offset()));
                        } catch (Exception ex) {
                            LOG.error("write to zk path : {}, data : {}, error : {}", new Object[]{dlp, metadata.offset(), ex});
                        }
                    } else {
                        LOG.error("write to kafka error", exception);
                    }
                }
            });
        } catch (Exception ex){
            LOG.error("write error", ex);
        }
    }


    private byte[] toByteArray(long offset){
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(offset);
        return buffer.array();
    }
}


