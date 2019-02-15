package com.owl.mq.server.push.service;

import com.owl.mq.client.transport.message.KafkaMessage;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.MessageCodec;
import com.owl.mq.server.bo.FastResendMessage;
import com.owl.mq.server.bo.ResendPacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Tboy
 */
public class MessageHolder {

    public static MessageHolder I = new MessageHolder();

    private static final ConcurrentHashMap<Long, FastResendMessage> MSG_MAPPER = new ConcurrentHashMap(1000);

    public static final PriorityBlockingQueue<ResendPacket> MSG_QUEUE = new PriorityBlockingQueue<>(1000);

    private static final AtomicLong MEMORY_SIZE = new AtomicLong(0);

    private static final AtomicLong COUNT = new AtomicLong(0);

    public static long memorySize(){
        return MEMORY_SIZE.get();
    }

    public static long count(){
        return COUNT.get();
    }

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static void fastPut(Packet packet){
        if(packet == null){
            return;
        }
        lock.writeLock().lock();
        try {
            KafkaMessage kafkaMessage = MessageCodec.decode(packet.getBody());
            MSG_QUEUE.put(new ResendPacket(kafkaMessage.getHeader().getMsgId()));
            long size = kafkaMessage.getHeaderInBytes().length + kafkaMessage.getKey().length + kafkaMessage.getValue().length;
            MSG_MAPPER.put(kafkaMessage.getHeader().getMsgId(), new FastResendMessage(kafkaMessage.getHeader().getMsgId(), kafkaMessage.getHeaderInBytes(), size));
            COUNT.incrementAndGet();
            MEMORY_SIZE.addAndGet(size);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static boolean fastRemove(KafkaMessage kafkaMessage){
        boolean result;
        lock.writeLock().lock();
        try {
            result = MSG_QUEUE.remove(new ResendPacket(kafkaMessage.getHeader().getMsgId()));
            FastResendMessage frm = MSG_MAPPER.remove(kafkaMessage.getHeader().getMsgId());
            if(frm != null){
                COUNT.decrementAndGet();
                MEMORY_SIZE.addAndGet(frm.getSize()*(-1));
            }
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
