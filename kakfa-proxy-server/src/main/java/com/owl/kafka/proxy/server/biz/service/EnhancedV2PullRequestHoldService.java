package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.util.CollectionUtils;
import com.owl.kafka.proxy.server.biz.bo.ManyPullRequest;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class EnhancedV2PullRequestHoldService{

    private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedV2PullRequestHoldService.class);

    private final Thread worker;

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final ManyPullRequest manyPullRequest = new ManyPullRequest();

    public EnhancedV2PullRequestHoldService(){
        this.start.compareAndSet(false, true);
        this.worker = new Thread(new Runnable() {
            @Override
            public void run() {
                while(start.get()){
                    try {
                        Thread.sleep(5 * 1000);
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
        this.manyPullRequest.add(pullRequest);
    }

    public void close(){
        start.compareAndSet(true, false);
        this.worker.interrupt();
        LOGGER.debug("close PullRequestHoldService ");
    }

    public void checkRequestHolder(){
        long start = System.currentTimeMillis();
        List<PullRequest> pullRequests = this.manyPullRequest.cloneAndClear();
        if(pullRequests != null){
            ArrayList<PullRequest> notExecuted = new ArrayList<>(pullRequests.size());
            for(PullRequest pullRequest : pullRequests){
                Packet result = PullCenter.I.pull(pullRequest, false);
                boolean execute = executeWhenWakeup(pullRequest, result);
                if(!execute){
                    notExecuted.add(pullRequest);
                }
            }
            if(!CollectionUtils.isEmpty(notExecuted)){
                this.manyPullRequest.add(notExecuted);
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
