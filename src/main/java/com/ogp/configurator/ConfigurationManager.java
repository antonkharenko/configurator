package com.ogp.configurator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ogp.configurator.serializer.ISerializer;

/**
* IConfigurationManagement interface implementation class.
* Provides direct save(), get(), delete(), list() methods to manipulate objects in Curator framework.
*
* @author Andriy Panasenko
*/
public class ConfigurationManager extends ConfigServiceCore implements IConfigurationManagement {
	private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);
	
	private volatile boolean isConnected;
	private ConfigurationManager(Builder builder) {
		super(builder);
		isConnected = false;
		
	}
	
	public static Builder newBuilder(CuratorFramework zkClient, ISerializer serializer, String environment) {
		return new Builder(zkClient, serializer, environment);
	}
	
	@Override
	public void start() {
		isConnected = getCurator().getZookeeperClient().isConnected();
		getCurator().getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				isConnected = newState.isConnected();
			}
		});

	}

	@Override
	public <T> void save(String key, T value) {
		if (value == null)
			throw new UnknownTypeException("value is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!isConnected()) {
			throw new ConnectionLossException("Config service not connected to ZooKeeper");
		}
		
		if (!isTypeConfigured(value.getClass()))
			throw new UnknownTypeException("Specified "+ value.getClass().toString()+" not registred");
		
		logger.trace("save() {}", value.toString());
		final String type = getType(value.getClass());
		final byte[] configSerialized = serialize(value);
		final String configEntityPath = buildPath(type, key);
		try {
			if (getCurator().checkExists().forPath(configEntityPath) == null)
				getCurator().create().forPath(configEntityPath, configSerialized);
			else
				getCurator().setData().forPath(configEntityPath, configSerialized);
		} catch (Exception e) {
			throw new InvalidAccessException(e);
		}
		logger.trace("save() ConfigEntity type{},key{}, json:{};", type, key, new String(configSerialized));
		
	}

	@Override
	public <T> void delete(Class<T> type, String key) {
		if (type == null)
			throw new UnknownTypeException("type is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!isConnected()) {
			throw new ConnectionLossException("Config service not connected to ZooKeeper");
		}
		
		if (!isTypeConfigured(type))
			throw new UnknownTypeException("Specified "+ type.toString()+" not registred");
		
		final String path = getPath(getType(type), key);
		try {
			getCurator().delete().forPath(path);
		} catch (Exception e) {
			throw new InvalidAccessException(e);
		}

	}

	@Override
	public <T> T get(Class<T> type, String key) {
		if (type == null)
			throw new UnknownTypeException("type is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!isConnected()) {
			throw new ConnectionLossException("Config service not connected to ZooKeeper");
		}
		
		if (!isTypeConfigured(type))
			throw new UnknownTypeException("Specified "+ type.toString()+" not registred");
		
		final String path = getPath(getType(type), key);
		
		try {
			if (getCurator().checkExists().forPath(path) == null) {
				logger.debug("get() not exist ConfigEntity type={}, key={}, path={}", type, key, path);
				return null;
			}
			final byte[] config = getCurator().getData().forPath(path);
			final T configDeserialized = deserialize(config, type);
			logger.trace("get() ConfigEntity type={},key={}, json:={};", type, key, new String(config));
			return configDeserialized;
		} catch (Exception e) {
			throw new InvalidAccessException(e);
		}
	}

	@Override
	public <T> List<T> list(Class<T> type) {
		if (type == null)
			throw new UnknownTypeException("type is null");
		
		if (!isTypeConfigured(type))
			throw new UnknownTypeException("Specified "+ type.toString()+" not registred");
		
		final String pathForType = getPathForType(getType(type));
		try {
			if (getCurator().checkExists().forPath(pathForType) == null) {
				logger.debug("list() not exist ConfigEntity type={}, path={}", type, pathForType);
				return new ArrayList<T>(0);
			}
			final List<String> keys = getCurator().getChildren().forPath(pathForType);
			byte[] config;
			final List<T> configList = new ArrayList<T>(keys.size());
			for (String keyPath : keys) {
				config = getCurator().getData().forPath(pathForType+CONFIG_PATH_DELIMITER+keyPath);
				logger.trace("list() adding ConfigEntity type={},key={}, json:={};", type, keyPath, new String(config));
				configList.add(deserialize(config, type));
			}
			return configList;
		} catch (Exception e) {
			throw new InvalidAccessException(e);
		}
	}

	@Override
	public boolean isConnected() {
		return isConnected;
	}

	@Override
	public void awaitConnected() throws InterruptedException {
		getCurator().blockUntilConnected();
	}

	@Override
	public boolean awaitConnected(long timeout, TimeUnit unit) throws InterruptedException {
		return getCurator().blockUntilConnected((int) timeout, unit);
	}

	public static class Builder extends ConfigServiceCore.Builder {

		public Builder(CuratorFramework zkClient, ISerializer serializer, String environment) {
			super(zkClient, serializer, environment);
		}

		public Builder registerConfigType(String type, Class<?> typeClass) {
			super.registerConfigType(type, typeClass);
			return this;
		}
		
		public ConfigurationManager build() {
			return new ConfigurationManager(this);
		}
	}
}
