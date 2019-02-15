package com.owl.mq.client.transport;


import com.owl.mq.client.transport.exceptions.ChannelInactiveException;
import com.owl.mq.client.util.KafkaPackets;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class ConnectionManager {

    private final ConcurrentHashMap<Address, ConnectionWatchDog> connections = new ConcurrentHashMap<>();

    public void manage(Address address, ConnectionWatchDog connectionWatchDog){
        connections.putIfAbsent(address, connectionWatchDog);
    }

    public Connection getConnection(Address address) {
        ConnectionWatchDog connectionWatchDog = connections.get(address);
        return connectionWatchDog.getConnection();
    }

    public void disconnect(Address address){
        ConnectionWatchDog connectionWatchDog = connections.remove(address);
        if(connectionWatchDog != null){
            connectionWatchDog.close();
        }
    }

    public void close(){
        Set<Map.Entry<Address, ConnectionWatchDog>> entries = connections.entrySet();
        for(Map.Entry<Address, ConnectionWatchDog> entry : entries){
            ConnectionWatchDog connectionWatchDog = entry.getValue();
            connectionWatchDog.setReconnect(false);
            try {
                entry.getValue().getConnection().send(KafkaPackets.unregister());
            } catch (ChannelInactiveException e) {
                //Ignore
            }
            connectionWatchDog.close();
        }
        connections.clear();
    }
}
