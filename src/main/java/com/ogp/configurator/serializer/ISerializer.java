package com.ogp.configurator.serializer;

public interface ISerializer {

	public byte[] serialize(Object obj) throws Exception;
	
	public <T> T deserialize(byte[] array, Class<T> clazz) throws Exception;
}
