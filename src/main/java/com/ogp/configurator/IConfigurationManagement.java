package com.ogp.configurator;

import rx.Observable;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ogp.configurator.serializer.SerializationException;

/**
 * @author Anton Kharenko
 */
public interface IConfigurationManagement {

	/**
	 * Start configuration management service and blocks until
	 * all configuration received and cached in local cache tree.
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 */
	void start() throws ConnectionLossException;

	/**
	 * Saves given configuration object at given key. If object with such key exists it
	 * will be replaced otherwise inserts new object.
	 *
	 * @param key configuration key
	 * @param config configuration object
	 * @param <T> class of configuration object
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 * @throws InvalidAccessException if not enough rights for write operation.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 * @throws SerializationException Runtime exception if something wrong happened in serializer. 
	 */
	<T> void save(String key, T config) throws ConnectionLossException;

	/**
	 * Deletes configuration object of the given type and with the given key.
	 *
	 * @param type configuration type
	 * @param key configuration key
	 * @param <T> class of configuration object
	 * @throws ConnectionLossException if not connected to the configuration storage.
	 * @throws InvalidAccessException if not enough rights for delete operation.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> void delete(Class<T> type, String key) throws ConnectionLossException;

	/**
	 * Returns configuration object of the given type under the given key or {@code null} if such
	 * object doesn't exists. This operation is always use locally cached instances and will return latest
	 * known values even when connection to configuration storage was lost.
	 *
	 * @param type configuration type
	 * @param key configuration key
	 * @param <T> class of configuration object
	 * @return Configuration object of the given type under the given key or {@code null} if such object doesn't exists.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> T get(Class<T> type, String key);

	/**
	 * Returns list of all configuration object for the given type or empty list if no such
	 * objects exists. This operation is always use locally cached instances and will return latest
	 * known values even when connection to configuration storage was lost.
	 *
	 * @param type configuration type
	 * @param <T> class of configuration object
	 * @return Returns list of all configuration object for the given type or empty list if no such objects exists.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> List<T> list(Class<T> type);

	/**
	 * Listens for all configuration updates events. It provides reactive approach for listening of configuration changes
	 * in contrast of polling getter methods.
	 *
	 * @return Observable which emits all configuration update events.
	 */
	Observable<ConfigurationUpdateEvent> listenUpdates();

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
