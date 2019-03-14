package com.owl.client.common.serializer;

import com.alibaba.fastjson.JSON;
import com.owl.client.common.util.Constants;

import java.nio.charset.Charset;


/**
 * @author Tboy
 */
public class FastJsonSerializer<T> implements Serializer<T> {

	@Override
	public byte[] serialize(T obj)  {
		if(obj == null){
			return null;
		}
		String json = JSON.toJSONString(obj);
		return json.getBytes(Constants.UTF8);
	}

	@Override
	public T deserialize(byte[] src, Class<T> clazz) {
		return JSON.parseObject(new String(src, Constants.UTF8), clazz);
	}

}
