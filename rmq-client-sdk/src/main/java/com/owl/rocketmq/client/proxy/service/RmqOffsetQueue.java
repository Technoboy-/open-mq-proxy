package com.owl.rocketmq.client.proxy.service;

import com.owl.mq.client.bo.TopicQueueOffset;
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
public class RmqOffsetQueue {

    private final ConcurrentHashMap<String/* msgId */, TopicQueueOffset> msgIdMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer/* queue */, TreeSet<TopicQueueOffset>> partitionMap = new ConcurrentHashMap<>();

    private final AtomicLong msgCount = new AtomicLong(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void put(List<RmqMessage> rmqMessages){
        this.lock.writeLock().lock();
        try {
            for(RmqMessage rmqMessage : rmqMessages){
                msgCount.incrementAndGet();
                TreeSet<TopicQueueOffset> offsetTreeSet = partitionMap.get(rmqMessage.getHeader().getQueue());
                if(offsetTreeSet == null){
                    offsetTreeSet = new TreeSet<>();
                    TreeSet<TopicQueueOffset> old = partitionMap.putIfAbsent(rmqMessage.getHeader().getQueue(), offsetTreeSet);
                    if(old != null){
                        offsetTreeSet = old;
                    }
                }
                TopicQueueOffset topicQueueOffset = new TopicQueueOffset(rmqMessage.getHeader().getBrokerName(), rmqMessage.getHeader().getTopic(),
                        rmqMessage.getHeader().getQueue(), rmqMessage.getHeader().getOffset(), rmqMessage.getHeader().getMsgId());
                offsetTreeSet.add(topicQueueOffset);
                msgIdMap.put(rmqMessage.getHeader().getMsgId(), topicQueueOffset);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public TopicQueueOffset remove(String msgId){
        TopicQueueOffset result = null;
        this.lock.writeLock().lock();
        try {
            TopicQueueOffset topicQueueOffset = msgIdMap.remove(msgId);
            if(topicQueueOffset != null){
                TreeSet<TopicQueueOffset> offsetTreeMap = partitionMap.get(topicQueueOffset.getQueue());
                if(offsetTreeMap != null){
                    offsetTreeMap.remove(topicQueueOffset);
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
