package com.owl.client.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

/**
 * @Author: Tboy
 */
public abstract class ClassPathPropertyLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassPathPropertyLoader.class);

    protected final Properties properties = new Properties();

    public ClassPathPropertyLoader(String fileName){
        load(fileName);
        toSystemProperties();
    }

    protected void load(String fileName){
        if(StringUtils.isEmpty(fileName) || !fileName.endsWith(".properties")){
            throw new IllegalArgumentException("fileName " + fileName + " is empty or not properties type");
        }
        InputStream fis = ClassPathPropertyLoader.class.getClassLoader().getResourceAsStream(fileName);
        if(fis == null){
            fis = ClassPathPropertyLoader.class.getResourceAsStream(fileName);
        }
        if(fis == null){
            throw new IllegalArgumentException("not found " + fileName + " in classpath");
        }
        try {
            properties.load(fis);
            toSystemProperties();
            afterLoad();
        } catch (Exception ex) {
            LOGGER.error("error load " + fileName, ex);
            throw new RuntimeException(ex);
        }
    }

    protected abstract void afterLoad();

    protected void toSystemProperties(){
        Enumeration<Object> keys = properties.keys();
        while(keys.hasMoreElements()){
            String key = keys.nextElement().toString();
            System.setProperty(key, properties.get(key).toString());
        }
    }

    protected int getInt(String key, int def){
        String value = get(key);
        if(StringUtils.isEmpty(value)){
            return def;
        }
        return Integer.valueOf(value);
    }

    protected long getLong(String key, long def){
        String value = get(key);
        if(StringUtils.isEmpty(value)){
            return def;
        }
        return Long.valueOf(value);
    }

    protected String get(String key, String def){
        String value = get(key);
        if(StringUtils.isEmpty(value)){
            return def;
        }
        return value;
    }

    protected String get(String key){
        return System.getProperty(key);
    }
}
