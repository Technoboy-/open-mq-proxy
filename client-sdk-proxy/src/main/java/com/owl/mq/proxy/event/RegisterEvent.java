package com.owl.mq.proxy.event;

/**
 * @Author: Tboy
 */
public class RegisterEvent implements Event {

    private int port;

    public RegisterEvent(){
        //NOP
    }

    public RegisterEvent(int port){
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
