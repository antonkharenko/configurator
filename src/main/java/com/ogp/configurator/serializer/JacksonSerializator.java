package com.ogp.configurator.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonSerializator implements ISerializer{

	private ObjectMapper mapper = new ObjectMapper();

	public byte[] Serialize(Object obj) throws Exception {
		return mapper.writeValueAsBytes(obj);
	}

	public <T> T Deserialize(byte[] array, Class<T> clazz) throws Exception
	{
		return mapper.readValue(array, clazz);
	}

}
