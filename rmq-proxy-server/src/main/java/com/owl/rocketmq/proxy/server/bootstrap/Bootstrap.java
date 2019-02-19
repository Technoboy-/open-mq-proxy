package com.owl.rocketmq.proxy.server.bootstrap;


import com.owl.rocketmq.proxy.server.pull.PullServer;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
//        System.setProperty("io.transport.leakDetection.level", "advanced");

        startPullServer();
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
