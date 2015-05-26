package com.ogp.configurator;

import com.ogp.configurator.serializer.SerializationException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This interface provides access to configuration storage for both read and write operations. All calls to this
 * interface is always a direct calls to to the underlying configuration storage.
 *
 * @author Anton Kharenko
 */
public interface IConfigurationManagementNew {

	/**
	 * Start configuration client. Most methods will not work until the client is started.
	 *
	 * @throws ConnectionLossException if not able to connect to the configuration storage.
	 */
	void start();

	/**
	 * Saves given configuration object under the given key. If object with such key exists it
	 * will be replaced otherwise inserts new object.
	 *
	 * @param key configuration object key
	 * @param value configuration object
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 * @throws SerializationException Runtime exception if something wrong happened in serializer.
	 */
	<T> void save(String key, T value);

	/**
	 * Deletes configuration object of the given type stored under the given key.
	 *
	 * @param type configuration object class
	 * @param key configuration object key
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> void delete(Class<T> type, String key);

	/**
	 * Returns configuration object of the given type stored under the given key or {@code null} if such
	 * object doesn't exists.
	 *
	 * @param type configuration object class
	 * @param key configuration object key
	 * @return Configuration object of the given type under the given key or {@code null} if such object doesn't exists.
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> T get(Class<T> type, String key);

	/**
	 * Returns list of all configuration object for the given type or empty list if no such
	 * objects exists.
	 *
	 * @param type configuration object class
	 * @return Returns list of all configuration object for the given type or empty list if no such objects exists.
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> List<T> list(Class<T> type);

	/**
	 * Returns the connection state to the configuration storage.
	 *
	 * @return true if connected.
	 */
	boolean isConnected();

	/**
	 * Causes the current thread to wait until connection to the configuration storage
	 * is established. If the connection is established then this method returns immediately.
	 *
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 */
	void awaitConnected() throws InterruptedException;

	/**
	 * Causes the current thread to wait until connection to the configuration storage
	 * is established, or the specified waiting time elapses. If the connection is established
	 * then this method returns immediately.
	 *
	 * @param timeout the maximum time to wait
	 * @param unit the time unit of the {@code timeout} argument
	 * @return {@code true} if connection was established and {@code false} if the waiting time elapsed.
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 */
	boolean awaitConnected(long timeout, TimeUnit unit) throws InterruptedException;
}
