package com.owl.mq.proxy.transport;

import com.owl.client.common.util.NamedThreadFactory;
import com.owl.mq.proxy.bo.ClientConfigs;
import com.owl.mq.proxy.bo.RegisterContent;
import com.owl.mq.proxy.transport.codec.PacketDecoder;
import com.owl.mq.proxy.transport.codec.PacketEncoder;
import com.owl.mq.proxy.transport.handler.ClientHandler;
import com.owl.mq.proxy.transport.handler.IdleStateTrigger;
import com.owl.mq.proxy.transport.handler.MessageDispatcher;
import com.owl.mq.proxy.util.Packets;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public abstract class NettyClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    private final IdleStateTrigger idleStateTrigger = new IdleStateTrigger();

    private final PacketEncoder encoder = new PacketEncoder();

    private final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("connector.timer"));

    private final ConnectionManager connectionManager = new ConnectionManager();

    private final Bootstrap bootstrap = new Bootstrap();

    private final NioEventLoopGroup workGroup;

    private final MessageDispatcher dispatcher = new MessageDispatcher();

    private ClientHandler handler;

    private final ClientConfigs clientConfigs;

    public NettyClient(ClientConfigs clientConfigs){
        this.clientConfigs = clientConfigs;
        this.workGroup = new NioEventLoopGroup(clientConfigs.getWorkerNum());
        this.bootstrap.
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.TCP_NODELAY, true).
                group(workGroup).
                channel(NioSocketChannel.class);
        initHandler(dispatcher);
        this.handler = new ClientHandler(this.dispatcher);
    }

    public abstract void initHandler(MessageDispatcher dispatcher);

    public void connect(Address address, boolean isSync) {
        //
        InetSocketAddress socketAddress = new InetSocketAddress(address.getHost(), address.getPort());
        final ConnectionWatchDog connectionWatchDog = new ConnectionWatchDog(bootstrap, timer, socketAddress){

            @Override
            public ChannelHandler[] handlers() {
                return new ChannelHandler[]{this,
                        new IdleStateHandler(0, 30, 0),
                        idleStateTrigger, new PacketDecoder(), handler, encoder};
            }

            @Override
            public void fireRegister() {
                RegisterContent registerContent = new RegisterContent(clientConfigs.getTopic(), clientConfigs.getGroupId());
                try {
                    getConnection().send(Packets.register(registerContent));
                } catch (Exception ex){
                    LOG.error("register fail, will retry laster", ex);
                    timer.newTimeout(new TimerTask(){

                        @Override
                        public void run(Timeout timeout) throws Exception {
                            getConnection().send(Packets.register(registerContent));
                        }
                    }, 100, TimeUnit.MILLISECONDS);

                }
            }


        };
        try {
            ChannelFuture future;
            synchronized (bootstrap){
                bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast((connectionWatchDog.handlers()));
                    }
                });
                future = bootstrap.connect(socketAddress);
            }
            if(isSync){
               future.sync();
            }
            connectionManager.manage(address, connectionWatchDog);
        } catch (Throwable ex){
            throw new RuntimeException("Connects to [" + address + "] fails", ex);
        }
    }

    public ConnectionManager getConnectionManager() {
        return this.connectionManager;
    }

    public void disconnect(Address address){
        try {
            connectionManager.disconnect(address);
        } catch (Throwable ex){
            LOG.error("disconnect error", ex);
        }
    }

    public void close(){
        this.connectionManager.close();
        this.timer.stop();
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }
}
