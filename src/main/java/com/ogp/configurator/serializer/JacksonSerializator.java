package com.ogp.configurator.serializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serialization/Deserialization interface implementation for work with Json using Jackson faster xml library.
 *
 * @author Andriy Panasenko
 */
public class JacksonSerializator implements ISerializer{

	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public <T> byte[] serialize(T obj) {
		try {
			return mapper.writeValueAsBytes(obj);
		} catch (JsonProcessingException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public <T> T deserialize(byte[] array, Class<T> clazz) {
		try {
			return mapper.readValue(array, clazz);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

}
