package com.ogp.configurator;

import rx.Observable;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * It provides read only access to configuration storage. Calls to this interface never do any blocking
 * network calls and always access locally cached configuration data. Configuration data is updated in an
 * event driven way by registering corresponding watchers to configuration storage and listen on
 * configuration update events.
 *
 * @author Anton Kharenko
 */
public interface IConfiguration {

	/**
	 * Start configuration client. Most methods will not work until the client is started. It doesn't block
	 * thread however getter won't return full state of configuration data until local replica is initialized.
	 * You can wait for initialized state either by polling {@code isInitialized()} method or listening for
	 * corresponding event.
	 *
	 * @throws ConnectionLossException if not able to connect to the configuration storage.
	 * @see #isInitialized();
	 */
	void start() throws ConnectionLossException;

	/**
	 * Returns either the local replica of configuration data is fully initialized.
	 *
	 * @return true if local replica of configuration data is initialized; false otherwise.
	 */
	boolean isInitialized();

	/**
	 * Causes the current thread to wait until local replica of configuration data is fully
	 * initialized. If it is already initialized then this method returns immediately.
	 *
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 */
	void awaitInitialized() throws InterruptedException;

	/**
	 * Causes the current thread to wait until local replica of configuration data is fully
	 * initialized, or the specified waiting time elapses. If it is already initialized
	 * then this method returns immediately.
	 *
	 * @param timeout the maximum time to wait
	 * @param unit the time unit of the {@code timeout} argument
	 * @return {@code true} if connection was established and {@code false} if the waiting time elapsed.
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 */
	boolean awaitInitialized(long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * Returns either exists connection to the configuration storage. Even if there is no connection to
	 * configuration storage all methods will return valid results for latest known data, but they can miss
	 * updates which happen after connection loss.
	 *
	 * @return true if connected.
	 */
	boolean isConnected();

	/**
	 * Returns configuration object of the given type stored under the given key or {@code null} if such
	 * object doesn't exists. This operation is always use locally cached instances and will return latest
	 * known values even when connection to configuration storage was lost.
	 *
	 * @param type configuration object class
	 * @param key configuration object key
	 * @return Configuration object of the given type under the given key or {@code null} if such object doesn't exists.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> T get(Class<T> type, String key);

	/**
	 * Returns list of all configuration object for the given type or empty list if no such
	 * objects exists. This operation is always use locally cached instances and will return latest
	 * known values even when connection to configuration storage was lost.
	 *
	 * @param type configuration object class
	 * @return Returns list of all configuration object for the given type or empty list if no such objects exists.
	 * @throws UnknownTypeException if given configuration type wasn't registered to the service.
	 */
	<T> List<T> list(Class<T> type);

	/**
	 * Listens for all configuration events emitted by persistent storage. It provides reactive approach for listening
	 * of configuration changes in contrast of polling getter methods.
	 *
	 * @return Observable which emits all configuration events.
	 */
	Observable<ConfigurationEvent> listen();

}
