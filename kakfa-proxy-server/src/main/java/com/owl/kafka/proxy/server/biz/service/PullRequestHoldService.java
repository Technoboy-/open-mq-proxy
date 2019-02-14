package com.owl.kafka.proxy.server.biz.service;

import com.owl.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.client.proxy.transport.protocol.Packet;
import com.owl.client.proxy.util.Packets;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class PullRequestHoldService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullRequestHoldService.class);

    private final ConcurrentHashMap<Long, PullRequest> requestHolder = new ConcurrentHashMap<>();

    private final Thread worker;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public PullRequestHoldService(){
        this.start.compareAndSet(false, true);
        this.worker = new Thread(new Runnable() {
            @Override
            public void run() {
                while(start.get()){
                    try {
                        Thread.sleep(3 * 1000);
                        checkRequestHolder();
                    } catch (InterruptedException e) {
                        //Ignore
                    }
                }
            }
        }, "PullRequestHoldService-thread");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void suspend(PullRequest pullRequest){
        requestHolder.put(pullRequest.getPacket().getOpaque(), pullRequest);
    }

    public void close(){
        this.start.compareAndSet(true, false);
        this.worker.interrupt();
        LOGGER.debug("close PullRequestHoldService ");
    }

    public void checkRequestHolder(){
        long start = System.currentTimeMillis();
        Iterator<Map.Entry<Long, PullRequest>> iterator = requestHolder.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<Long, PullRequest> next = iterator.next();
            PullRequest request = next.getValue();
            Packet result = PullCenter.I.pull(request, false);
            boolean execute = executeWhenWakeup(next.getValue(), result);
            if(execute){
                iterator.remove();
            }
        }
        long takes = System.currentTimeMillis() - start;
        if(takes > 1){
            LOGGER.error("checkRequestHolder() takes {} ms", takes);
        }
    }

    public void notifyMessageArriving(){
        checkRequestHolder();
    }

    private boolean executeWhenWakeup(PullRequest request, Packet result){
        boolean execute = false;
        try {
            if(!result.isBodyEmtpy()){
                request.getConnection().send(result);
                execute = true;
            } else if(System.currentTimeMillis() > (request.getSuspendTimestamp() + request.getTimeoutMs())){
                final Packet packet = Packets.pullNoMsgResp(request.getPacket().getOpaque());
                request.getConnection().send(packet);
                execute = true;
            }
        } catch (ChannelInactiveException e) {
            LOGGER.warn("ChannelInactiveException, ignore", e);
            execute = true;
        }
        return execute;
    }
}
