package com.ogp.configurator;

/**
 * Exception which indicates that was attempt to perform operation while it is not allowed by access rights
 * to that instance of configuration service.
 *
 * @author Anton Kharenko
 */
public class InvalidAccessException extends RuntimeException {

	public InvalidAccessException(String message) {
		super(message);
	}

	public InvalidAccessException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidAccessException(Throwable cause) {
		super(cause);
	}

}
