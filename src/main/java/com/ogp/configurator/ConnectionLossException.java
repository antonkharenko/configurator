package com.ogp.configurator;

/**
 * Exception which indicates that update operation attempt were performed while connection to
 * configuration storage was lost.
 *
 * @author Anton Kharenko
 */
public class ConnectionLossException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7097884642255639396L;

	public ConnectionLossException(String message) {
		super(message);
	}

	public ConnectionLossException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConnectionLossException(Throwable cause) {
		super(cause);
	}
}
