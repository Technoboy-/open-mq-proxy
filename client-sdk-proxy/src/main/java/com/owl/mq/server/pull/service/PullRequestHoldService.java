package com.owl.mq.server.pull.service;

import com.owl.client.common.util.CollectionUtils;
import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.KafkaPackets;
import com.owl.mq.server.bo.ManyPullRequest;
import com.owl.mq.server.bo.PullRequest;
import com.owl.mq.server.pull.AbstractPullCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class PullRequestHoldService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullRequestHoldService.class);

    private final Thread worker;

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final ManyPullRequest manyPullRequest = new ManyPullRequest();

    private final AbstractPullCenter abstractPullCenter;

    public PullRequestHoldService(AbstractPullCenter abstractPullCenter){
        this.abstractPullCenter = abstractPullCenter;
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
                Packet result = this.abstractPullCenter.pull(pullRequest, false);
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
                final Packet packet = KafkaPackets.pullNoMsgResp(request.getPacket().getOpaque());
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
