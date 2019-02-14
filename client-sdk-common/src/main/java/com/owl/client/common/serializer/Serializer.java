package com.owl.client.common.serializer;


import com.owl.client.common.serializer.spi.SPI;

/**
 * @Author: Tboy
 */
@SPI("bytearray")
public interface Serializer<T> {

    byte[] serialize(T obj);

    T deserialize(byte[] src, Class<T> clazz);

}
