package com.owl.kafka.proxy.server.boostrap;

import com.owl.kafka.proxy.server.pull.PullServer;
import com.owl.kafka.proxy.server.push.PushServer;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
//        System.setProperty("io.transport.leakDetection.level", "advanced");

        startPullServer();
//        startPushServer();
    }

    private static void startPullServer(){
        //
        PullServer pullServer = new PullServer();
        //
        pullServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pullServer.close();
        }));
    }

    private static void startPushServer(){
        //
        PushServer pushServer = new PushServer();
        //
        pushServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pushServer.close();
        }));
    }


}
