package com.owl.rocketmq.client.proxy.service;

import com.owl.mq.client.bo.TopicPartitionOffset;
import com.owl.mq.client.transport.message.RmqMessage;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Tboy
 */
public class OffsetQueue {

    private final ConcurrentHashMap<Long/* msgId */, TopicPartitionOffset> msgIdMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer/* parition */, TreeSet<TopicPartitionOffset>> partitionMap = new ConcurrentHashMap<>();

    private final AtomicLong msgCount = new AtomicLong(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void put(List<RmqMessage> rmqMessages){
//        this.lock.writeLock().lock();
//        try {
//            for(RmqMessage rmqMessage : rmqMessages){
//                msgCount.incrementAndGet();
//                TreeSet<TopicPartitionOffset> offsetTreeSet = partitionMap.get(rmqMessage.getHeader().getPartition());
//                if(offsetTreeSet == null){
//                    offsetTreeSet = new TreeSet<>();
//                    TreeSet<TopicPartitionOffset> old = partitionMap.putIfAbsent(kafkaMessage.getHeader().getPartition(), offsetTreeSet);
//                    if(old != null){
//                        offsetTreeSet = old;
//                    }
//                }
//                TopicPartitionOffset topicPartitionOffset = new TopicPartitionOffset(kafkaMessage.getHeader().getTopic(),
//                        kafkaMessage.getHeader().getPartition(), kafkaMessage.getHeader().getOffset(), kafkaMessage.getHeader().getMsgId());
//                offsetTreeSet.add(topicPartitionOffset);
//                msgIdMap.put(kafkaMessage.getHeader().getMsgId(), topicPartitionOffset);
//            }
//        } finally {
//            this.lock.writeLock().unlock();
//        }
    }

    public TopicPartitionOffset remove(Long msgId){
        TopicPartitionOffset result = null;
        this.lock.writeLock().lock();
        try {
            TopicPartitionOffset topicPartitionOffset = msgIdMap.remove(msgId);
            if(topicPartitionOffset != null){
                TreeSet<TopicPartitionOffset> offsetTreeMap = partitionMap.get(topicPartitionOffset.getPartition());
                if(offsetTreeMap != null){
                    offsetTreeMap.remove(topicPartitionOffset);
                }
                if(!offsetTreeMap.isEmpty()){
                    result = offsetTreeMap.first();
                }
                msgCount.decrementAndGet();
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return result;
    }

    public long getMessageCount() {
        return msgCount.get();
    }


}
