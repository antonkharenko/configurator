package com.ogp.configurator;

import rx.Observable;

/**
* It provides configuration data updates in an event driven way by registering 
* corresponding watchers to configuration storage and listen on configuration update events.
*
* @author Andriy Panasenko
*/
public interface IConfigurationMonitor {
	
	/**
	 * Start configuration client. Most methods will not work until the client is started. It doesn't block
	 * thread however getters won't return full state of configuration data until local replica is initialized.
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
	 * Returns either exists connection to the configuration storage. Even if there is no connection to
	 * configuration storage all methods will return valid results for latest known data, but they can miss
	 * updates which happen after connection loss.
	 *
	 * @return true if connected.
	 */
	boolean isConnected();
	
	/**
	 * Listens for all configuration events emitted by persistent storage. It provides reactive approach for listening
	 * of configuration changes in contrast of polling getter methods.
	 *
	 * @return Observable which emits all configuration events.
	 */
	Observable<ConfigurationEvent> listen();

}
