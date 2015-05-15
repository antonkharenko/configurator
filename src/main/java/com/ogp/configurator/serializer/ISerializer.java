package com.ogp.configurator.serializer;

public interface ISerializer {

	public byte[] Serialize(Object obj) throws Exception;
	
	public <T> T Deserialize(byte[] array, Class<T> clazz) throws Exception;
}
