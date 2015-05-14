package com.ogp.configurator;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonSerializator {

	private static ObjectMapper mapper = new ObjectMapper();

	public static byte[] Serialize(Object obj) throws JsonProcessingException {
		return mapper.writeValueAsBytes(obj);
	}

	public static Object Deserialize(byte[] array, Class<Object> clazz)
			throws JsonProcessingException, JsonMappingException, IOException
	{
		return mapper.readValue(array, clazz);
	}

}
