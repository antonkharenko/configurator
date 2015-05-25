package com.ogp.configurator;

/**
 * Exception which indicates that provided configuration type wasn't registered to the service.
 *
 * @author Anton Kharenko
 */
public class UnknownTypeException extends RuntimeException {

	/**
	 *
	 */
	private static final long serialVersionUID = 3124237997999572551L;

	public UnknownTypeException(String message) {
		super(message);
	}

	public UnknownTypeException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnknownTypeException(Throwable cause) {
		super(cause);
	}

}
