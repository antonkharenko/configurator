package com.ogp.configurator.serializer;

/**
 * Exception which indicates that something wrong in serialization/deserialization
 *
 * @author Andriy Panasenko
 */
public class SerializationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5452663994584739597L;

	public SerializationException(String message) {
		super(message);
	}
	
	public SerializationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public SerializationException(Throwable cause) {
		super(cause);
	}
}
