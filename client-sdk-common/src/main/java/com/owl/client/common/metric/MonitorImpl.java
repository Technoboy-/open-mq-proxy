package com.owl.client.common.metric;


import com.owl.client.common.metric.spi.MonitorConfig;
import com.owl.client.common.metric.spi.MonitorLoader;

/**
 * @Author: Tboy
 */
public class MonitorImpl{

    public static Monitor getDefault() {
        return MonitorLoader.getSPIClass(Monitor.class, MonitorConfig.class).getExtension();
    }

    public static Monitor getFileMonitor() {
        return MonitorLoader.getSPIClass(Monitor.class, MonitorConfig.class).getExtension("file");
    }

}
