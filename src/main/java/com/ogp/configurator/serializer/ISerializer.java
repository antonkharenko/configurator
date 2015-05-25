package com.ogp.configurator.serializer;


/**
 * @author Andriy Panasenko
 */
public interface ISerializer {

	/**
	 * Serialize given object to byte[]
	 *
	 * @param <T> Serialization Object
	 * @return byte[] serialized byte array
	 * @throws SerializationException Runtime exception if something wrong happened in serializer.
	 */
	public <T> byte[] serialize(T obj);
	
	/**
	 * Deserialize given byte[] array to Object of Class<T>
	 *
	 * @param byte[] byte array
	 * @param Class <T> of Deserialization Object
	 * @throws SerializationException Runtime exception if something wrong happened in serializer.
	 */
	public <T> T deserialize(byte[] array, Class<T> clazz);
}
