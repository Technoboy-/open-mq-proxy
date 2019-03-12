package com.owl.kafka.proxy.server.boostrap;

import com.owl.kafka.proxy.server.pull.PullServer;

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


}
