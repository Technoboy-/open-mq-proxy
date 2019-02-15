package com.owl.mq.server.push;

import com.owl.mq.client.service.DefaultRetryPolicy;
import com.owl.mq.client.service.RetryPolicy;
import com.owl.mq.client.transport.Connection;
import com.owl.mq.client.transport.alloc.ByteBufferPool;
import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.transport.protocol.Packet;
import com.owl.mq.server.bo.ControlResult;
import com.owl.mq.server.bo.ServerConfigs;
import com.owl.mq.server.push.service.*;
import com.owl.mq.server.registry.RegistryCenter;
import com.owl.mq.server.service.InstanceHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public abstract class AbstractPushCenter<T> implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPushCenter.class);

    private final int queueSize = ServerConfigs.I.getServerQueueSize();

    private final LoadBalance<Connection> loadBalance = new RoundRobinLoadBalance();

    private final RetryPolicy retryPolicy = new DefaultRetryPolicy();

    protected final ArrayBlockingQueue<Packet> retryQueue = new ArrayBlockingQueue<>(queueSize);

    protected final ArrayBlockingQueue<T> pushQueue = new ArrayBlockingQueue<>(queueSize);

    private final FlowController flowController = new DefaultFlowController();

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final Thread worker;

    protected final ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    public AbstractPushCenter(){
        this.worker = new Thread(this,"push-worker");
        this.worker.setDaemon(true);
    }

    public void start(){
        this.start.compareAndSet(false, true);
        this.worker.start();
    }

    public void putMessage(T record) throws InterruptedException{
        this.pushQueue.put(record);
    }

    public void push(Packet packet) throws InterruptedException, ChannelInactiveException {
        checkState();
        this.push(packet, new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(future.isSuccess()){
                    MessageHolder.fastPut(packet);
                } else {
                    retryQueue.put(packet);
                }
            }
        });
    }

    //TODO flow control and loadbalance can be optimized by using wait, notify
    private void push(Packet packet, final ChannelFutureListener listener) throws InterruptedException, ChannelInactiveException {
        ControlResult controlResult = flowController.flowControl(packet);
        while(!controlResult.isAllowed()){
            controlResult = flowController.flowControl(packet);
        }
        retryPolicy.reset();
        Connection connection = loadBalance.select(InstanceHolder.I.get(RegistryCenter.class).getClientRegistry().getClients());
        while((connection == null && retryPolicy.allowRetry()) || (!connection.isWritable() && !connection.isActive())){
            connection = loadBalance.select(InstanceHolder.I.get(RegistryCenter.class).getClientRegistry().getClients());
        }

        //
        connection.send(packet, listener);
    }

    @Override
    public void run() {
        while(this.start.get()){
            Packet ref = null;
            try {
                final Packet packet = take();
                ref = packet;
                push(packet);
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException", ex);
            } catch (ChannelInactiveException ex){
                LOGGER.error("ChannelInactiveException", ex);
                if(ref != null){
                    putOrPush(ref);
                }
            }
        }
    }

    private void putOrPush(Packet ref){
        if(Thread.currentThread().getName().startsWith("push-worker")){
            try {
                boolean offer = retryQueue.offer(ref, 50, TimeUnit.MILLISECONDS);
                if(!offer){
                    push(ref);
                }
            } catch (InterruptedException e) {

            } catch (ChannelInactiveException e) {
                putOrPush(ref);
            }
        } else{
            try {
                retryQueue.put(ref);
            } catch (InterruptedException e) {}
        }
    }

    public abstract Packet take() throws InterruptedException;

//    private Packet take() throws InterruptedException{
//        Packet packet = retryQueue.peek();
//        if(packet != null){
//            retryQueue.poll();
//        } else{
//            ConsumerRecord<byte[], byte[]> record = pushQueue.take();
//            packet = new Packet();
//            //
//            packet.setCmd(Command.PUSH.getCmd());
//            packet.setOpaque(IdService.I.getId());
//
//            KafkaHeader header = new KafkaHeader(record.topic(), record.partition(), record.offset(), IdService.I.getId());
//            header.setSign(KafkaHeader.Sign.PUSH.getSign());
//            byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
//            //
//            ByteBuf buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + record.key().length + 4 + record.value().length);
//            buffer.writeInt(headerInBytes.length);
//            buffer.writeBytes(headerInBytes);
//            //
//            buffer.writeInt(record.key().length);
//            buffer.writeBytes(record.key());
//            //
//            buffer.writeInt(record.value().length);
//            buffer.writeBytes(record.value());
//            //
//            packet.setBody(buffer);
//        }
//        return packet;
//    }

    private void checkState(){
        if(!start.get()){
            throw new IllegalStateException("push center not start");
        }
    }

    public void close() {
        this.start.compareAndSet(true, false);
        this.worker.interrupt();
    }
}
