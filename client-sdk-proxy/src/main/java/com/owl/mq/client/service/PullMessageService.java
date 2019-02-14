package com.owl.mq.client.service;


import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.client.bo.ClientConfigs;
import com.owl.mq.client.DefaultPullMessageImpl;
import com.owl.mq.client.transport.Address;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.NettyClient;
import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.client.util.Packets;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public abstract class PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullMessageImpl.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("scheduled-pull-message-service"));

    private final NettyClient nettyClient;

    private final int pullTimeoutMs = 30 * 1000;

    private final int processQueueSize = ClientConfigs.I.getProcessQueueSize();

    private final CopyOnWriteArraySet<Address> addresses = new CopyOnWriteArraySet<>();

    public PullMessageService(NettyClient nettyClient){
        this.nettyClient = nettyClient;
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                InvokerPromise.scan();
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    public void startPull(Address address){
        this.addresses.add(address);
        this.pullImmediately(address);
    }

    public void stopPull(Address address){
        this.addresses.remove(address);
    }

    public abstract boolean checkIfPullImmediately(Address address);

    private void pullImmediately(Address address){
        if(!addresses.contains(address)){
            LOGGER.warn("stop pull due to address : {} not register", address);
            return;
        }
        if(!checkIfPullImmediately(address)){
            pullLater(address);
            return;
        }
        Connection connection = nettyClient.getConnectionManager().getConnection(address);
        if(connection == null || !connection.isActive()){
            LOGGER.warn("connection is inactive, pull laster", address);
            pullLater(address);
            return;
        }
        long opaque = IdService.I.getId();
        PullCallback callback = new PullCallback(){

            @Override
            public void onComplete(Packet packet) {
                pullImmediately(address);
            }

            @Override
            public void onException(Throwable ex) {
                LOGGER.error("exception on pull ", ex);
                pullLater(address);
            }
        };
        try {
            connection.send(Packets.pullReq(opaque), new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    Throwable ex = future.cause();
                    new InvokerPromise(opaque, pullTimeoutMs, new InvokeCallback() {
                        @Override
                        public void onComplete(InvokerPromise invokerPromise) {
                            Packet response = invokerPromise.getResult();
                            if(response != null){
                                callback.onComplete(response);
                            } else if(invokerPromise.isTimeout()){
                                callback.onException(new TimeoutException("timeout exception"));
                            } else{
                                callback.onException(ex == null ? new Exception("unknown exception") : ex);
                            }
                        }
                    });
                }
            });
        } catch (ChannelInactiveException ex) {
            LOGGER.warn("ChannelInactiveException", ex);
            pullLater(address);
        }
    }

    private void pullLater(Address address){
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                pullImmediately(address);
            }
        }, 3000, TimeUnit.MILLISECONDS);
    }

    public void close(){
        scheduledExecutorService.shutdown();
    }
}
