package com.owl.mq.server.pull;

import com.owl.mq.client.transport.alloc.ByteBufferPool;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.server.bo.PullRequest;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.pull.service.PullRequestHoldService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @Author: Tboy
 */
public abstract class AbstractPullCenter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullCenter.class);

    private final int queueSize = ServerConfigs.I.getServerQueueSize();

    protected final ArrayBlockingQueue<Packet> retryQueue = new ArrayBlockingQueue<>(queueSize);

    protected final ArrayBlockingQueue<T> pullQueue = new ArrayBlockingQueue<>(queueSize);

    private final int pullMessageCount = ServerConfigs.I.getServerPullMessageCount();

    private final long messageSize = ServerConfigs.I.getServerPullMessageSize();

    private final PullRequestHoldService pullRequestHoldService = new PullRequestHoldService(this);

    protected final ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    public void putMessage(T record) throws InterruptedException{
        this.pullQueue.put(record);
        this.pullRequestHoldService.notifyMessageArriving();
    }

    public void reputMessage(Packet packet) throws InterruptedException{
        this.retryQueue.put(packet);
        this.pullRequestHoldService.notifyMessageArriving();
    }

    public Packet pull(PullRequest request, boolean isSuspend) {
        long messageCount = pullMessageCount;
        final Packet result = request.getPacket();
        while(messageCount > 0 && result.getBodyLength() < messageSize * pullMessageCount){
            messageCount--;
            if(!this.poll(result)){
                break;
            }
        }
        if(result.isBodyEmtpy() && isSuspend){
            pullRequestHoldService.suspend(request);
        }
        return result;
    }

    public abstract Packet pullNoMsgResp(long opaque);

    public abstract boolean poll(Packet packet);

    public void close(){
        this.pullRequestHoldService.close();
    }
}
