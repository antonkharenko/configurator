package com.ogp.configurator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ogp.configurator.ConfigurationEvent.ConfigType;
import com.ogp.configurator.ConfigurationEvent.UpdateType;
import com.ogp.configurator.serializer.ISerializer;
import com.ogp.configurator.serializer.SerializationException;
import rx.Observable;
import rx.subjects.PublishSubject;

/**
* IConfigurationMonitor interface implementation class.
* Provides Observable interface to object updates events.
*
* @author Andriy Panasenko
*/
public class ConfigurationMonitor extends ConfigServiceCore implements IConfigurationMonitor {

	private static final Logger logger = LoggerFactory.getLogger(ConfigurationMonitor.class);
	private final TreeCache configCache;
	private final PublishSubject<ConfigurationEvent> subject;
	private volatile boolean isInitialized; // switch to true after initialization complete, used during startup
	private volatile boolean isConnected;
	
	
	protected ConfigurationMonitor(Builder builder) {
		super(builder);
		this.subject = PublishSubject.create();
		this.configCache = new TreeCache(getCurator(), configEnvironmentPath);
		this.configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				logger.trace("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
				processEvent(event);
			}
		}); 
		this.isInitialized = false;
		this.isConnected = false;
	}
	
	public static Builder newBuilder(CuratorFramework zkClient, ISerializer serializer, String environment) {
		return new Builder(zkClient, serializer, environment);
	}
	
	@Override
	public void start() throws ConnectionLossException {
		try {
			configCache.start();
			this.isConnected = true;
		} catch (Exception e) {
			logger.error("Failed to TreeCache", e);
			throw new ConnectionLossException(e);
		}
	}

	@Override
	public boolean isInitialized() {
		return isInitialized;
	}

	@Override
	public boolean isConnected() {
		return isConnected;
	}

	@Override
	public Observable<ConfigurationEvent> listen() {
		return subject;
	}

	private Class<? extends Object> childDataToClass(ChildData childData) {
		if (childData == null) {
			return null;
		}
		String path = childData.getPath();
		String[] pathElements = path.split(CONFIG_PATH_DELIMITER);
		if (pathElements.length > 1) {
			String type = pathElements[pathElements.length-2];
			return getClass(type);
		}
		logger.debug("childDataToClass() to small elements in path={}", path); 
		return null;
	}
	
	private String childDataToKey(ChildData childData) {
		if (childData == null) {
			return null;
		}
		String path = childData.getPath();
		String[] pathElements = path.split(CONFIG_PATH_DELIMITER);
		if (pathElements.length > 1) {
			String type = pathElements[pathElements.length-1];
			return type;
		}
		logger.debug("childDataToKey() to small elements in path={}", path);
		return null;
	}
	
	private static String childDataToString(ChildData childData) {
		if (childData == null) {
			return "null";
		} else {
			String dataAsString = childData.getData() == null  ? "null" : new String(childData.getData());
			return "path=" + childData.getPath() + " stat=" + childData.getStat() + " data=" + dataAsString;
		}
	}
	
	private void processEvent(TreeCacheEvent event) throws Exception {
		Class<? extends Object> configEntityClass = childDataToClass(event.getData());
		
		logger.trace("processEvent() type={}", event.getType().toString());

		boolean isAdd = false;
		switch (event.getType()) {
			case NODE_ADDED:
				isAdd = true;
			case NODE_UPDATED:
				logger.trace("processEvent() path={}", event.getData().getPath());
				if (configEntityClass != null) {
					logger.trace("processEvent() got update: class={}, data {}", configEntityClass.toString(), new String(event.getData().getData()));
					try {
						Object newObj = deserialize(event.getData().getData(), configEntityClass);
						String key = childDataToKey(event.getData());
						if (isAdd) {
							subject.onNext(new ConfigurationEvent(key, configEntityClass, key, null, newObj, UpdateType.ADDED));
							logger.trace("ConfigurationMonitor() add new object, key=({}), class={}", key, configEntityClass.toString());
						} else {
							subject.onNext(new ConfigurationEvent(key, configEntityClass, key, null, newObj, UpdateType.UPDATED));
							logger.trace("ConfigurationMonitor() update object, key=({}), class={}", key, configEntityClass.toString());
						}
					} catch (SerializationException e) {
						logger.warn("Configuration node at path={}, key={}, class={}, have incorrect data, ignoring update.",
								event.getData().getPath(),
								childDataToKey(event.getData()),
								configEntityClass.toString());
					}
				}
				break;
			case NODE_REMOVED:
				logger.trace("processEvent() path={}", event.getData().getPath());
				if (configEntityClass != null) {
					String key = childDataToKey(event.getData());
					subject.onNext(new ConfigurationEvent(key, configEntityClass, key, null, null, UpdateType.REMOVED));
					logger.trace("ConfigurationMonitor() remove object, key=({}), class={}", key, configEntityClass.toString());
				}
				break;
			case INITIALIZED:
				logger.info("Initialization complete");
				isInitialized = true;
				subject.onNext(new ConfigurationEvent(ConfigType.INITIALIZED));
				break;
			case CONNECTION_LOST:
			case CONNECTION_SUSPENDED:
				isConnected = true;
				logger.info("Connection with ZooKeeper lost");
				subject.onNext(new ConfigurationEvent(ConfigType.CONNECTION_LOST));
				break;
			case CONNECTION_RECONNECTED:
				isConnected = false;
				logger.info("Connection with ZooKeeper restored");
				subject.onNext(new ConfigurationEvent(ConfigType.CONNECTION_RESTORED));
				break;
		}		
	}
	
	public static class Builder extends ConfigServiceCore.Builder {

		public Builder(CuratorFramework zkClient, ISerializer serializer, String environment) {
			super(zkClient, serializer, environment);
		}

		public Builder registerConfigType(String type, Class<?> typeClass) {
			super.registerConfigType(type, typeClass);
			return this;
		}
		
		public ConfigurationMonitor build() {
			return new ConfigurationMonitor(this);
		}
	}
}
