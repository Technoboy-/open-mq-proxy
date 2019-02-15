package com.owl.client.common.metric;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.NullConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * @Author: Tboy
 */
public class OutputLog {

    private  Logger logger;

    public OutputLog(String name){
        Configuration configuration = new NullConfiguration();
        try {
            PatternLayout patternLayout = PatternLayout.createDefaultLayout();
            RollingFileAppender appender = RollingFileAppender
                    .newBuilder()
                    .withLayout(patternLayout)
                    .withFileName(name)
                    .withFilePattern(name + "-%d{yyyy-MM-dd-HH}-%i.log.gz")
                    .withPolicy(SizeBasedTriggeringPolicy.createPolicy("10M"))
                    .withName(name)
                    .withAppend(true)
                    .withBufferedIo(true)
                    .build();
            appender.start();
            LoggerConfig loggerConfig = new LoggerConfig("monitor-config", Level.INFO, false);
            loggerConfig.addAppender(appender, null, null);
            configuration.addLogger("monitor", loggerConfig);
            //
            LoggerContext loggerContext = new LoggerContext("monitor-context");
            loggerContext.start(configuration);

            this.logger = loggerContext.getLogger("monitor");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void info(String message){
        this.logger.info(message);
    }

}
