package com.owl.kafka.proxy.server.transport.handler;


import com.owl.client.proxy.transport.Connection;
import com.owl.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.client.proxy.transport.protocol.Command;
import com.owl.client.proxy.transport.protocol.Packet;
import com.owl.client.proxy.util.ChannelUtils;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PullReqMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullReqMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received pull request : {}, from : {}", packet, ChannelUtils.getRemoteAddress(connection.getChannel()));
        }
        packet.setCmd(Command.PULL_RESP.getCmd());
        PullRequest pullRequest = new PullRequest(connection, packet, 15 * 1000);
        Packet result = PullCenter.I.pull(pullRequest, true);
        //
        if(!result.isBodyEmtpy()){
            try {
                connection.send(result);
            } catch (ChannelInactiveException ex){
                PullCenter.I.reputMessage(result);
            }
        }
    }

}
