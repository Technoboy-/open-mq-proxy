package com.owl.mq.client.transport.handler;

import com.owl.mq.client.util.KafkaPackets;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @Author: Tboy
 */
@ChannelHandler.Sharable
public class IdleStateTrigger extends ChannelInboundHandlerAdapter {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if(evt instanceof IdleStateEvent){
            IdleStateEvent event = (IdleStateEvent)evt;
            if(event.state() == IdleState.WRITER_IDLE){
                ctx.writeAndFlush(KafkaPackets.pingContent());
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }


}
