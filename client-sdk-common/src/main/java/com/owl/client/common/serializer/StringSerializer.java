package com.owl.client.common.serializer;


import com.owl.client.common.util.StringUtils;

import java.nio.charset.Charset;

/**
 * @Author: Tboy
 */
public class StringSerializer implements Serializer<String> {

    Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public byte[] serialize(String obj) {
        if(StringUtils.isEmpty(obj)){
            return null;
        }
        return obj.getBytes(UTF8);
    }

    @Override
    public String deserialize(byte[] src, Class<String> clazz) {
        return new String(src, UTF8);
    }
}
