package com.owl.mq.proxy.transport.protocol;

/**
 * @Author: Tboy
 */
public enum Command {

    REGISTER(1),

    PING(2),

    PONG(3),

    PULL_REQ(4),

    PULL_RESP(5),

    ACK(6),

    VIEW_REQ(7),

    VIEW_RESP(8),

    SEND_BACK(9),

    UNREGISTER(10),

    UNKNOWN(-1);

    Command(int cmd) {
        this.cmd = (byte) cmd;
    }

    public final byte cmd;

    public static Command toCMD(byte b) {
        Command[] values = values();
        if (b > 0 && b < values.length)
            return values[b - 1];
        return UNKNOWN;
    }

    public byte getCmd() {
        return cmd;
    }
}
