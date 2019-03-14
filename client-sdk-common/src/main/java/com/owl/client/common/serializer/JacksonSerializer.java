package com.owl.client.common.serializer;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @Author: Tboy
 */
public class JacksonSerializer<T> implements Serializer<T> {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    static{
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public byte[] serialize(T obj) {
        if(obj == null){
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public T deserialize(byte[] src, Class<T> clazz) {
        try {
            return (T)objectMapper.readValue(src, 0, src.length, clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


}
