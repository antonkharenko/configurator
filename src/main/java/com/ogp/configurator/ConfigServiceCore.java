package com.ogp.configurator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.ogp.configurator.serializer.ISerializer;

/**
* Configuration Service core abstract class.
* Provides base functions for work with configurations path.
*
* @author Andriy Panasenko
*/
public abstract class ConfigServiceCore {
	public static final String CONFIG_PATH_DELIMITER = "/";
	
	private static final Logger logger = LoggerFactory.getLogger(ConfigServiceCore.class);

	private static final String CONFIG_BASE_PATH = CONFIG_PATH_DELIMITER+"config";

	private final CuratorFramework curator;
	protected final String configEnvironmentPath;
	private final ISerializer serializer;
	private final Map<String, Class<Object>> configTypes;
	private final Map<Class<Object>, String> classToType;
	
	public static Builder newBuilder(CuratorFramework zkClient, ISerializer serializer, String environment) {
		return new Builder(zkClient, serializer, environment);
	}

	protected ConfigServiceCore(Builder builder){
		this.curator = builder.curator;
		this.serializer = builder.serializer;
		this.configEnvironmentPath = CONFIG_BASE_PATH + CONFIG_PATH_DELIMITER + builder.environment;
		this.configTypes = ImmutableMap.copyOf(builder.configTypes);
		
		// TODO: refactoring
		Map<Class<Object>, String> classToType = new HashMap<>(configTypes.size());
		for (Map.Entry<String, Class<Object>> typeEntry : configTypes.entrySet()) {
			logger.trace("key={}, class={} configured", typeEntry.getKey(), typeEntry.getValue());
			classToType.put(typeEntry.getValue(), typeEntry.getKey());
		}
		this.classToType = ImmutableMap.copyOf(classToType);

		initPaths();
		
	}
	
	protected boolean isTypeConfigured(Class<? extends Object> clazz) {
		return classToType.containsKey(clazz);
	}
	
	protected String getType(Class<? extends Object> clazz) {
		return classToType.get(clazz);
	}
	
	protected Class<? extends Object> getClass(String type) {
		return configTypes.get(type);
	}
	
	protected String getPathForType(String type) {
		return configEnvironmentPath + CONFIG_PATH_DELIMITER + type;
	}
	
	protected String getPath(String type, String key) {
		return getPathForType(type) + CONFIG_PATH_DELIMITER + key;
	}
	
	protected String buildPath(String type, String key) {
		ensurePath(getPathForType(type));
		return getPath(type, key);
	}
	
	protected <T> byte[] serialize(T value) {
		return serializer.serialize(value);
	}
	
	protected <T> T deserialize(byte[] array, Class<T> clazz) {
		return serializer.deserialize(array, clazz);
	}
	
	protected CuratorFramework getCurator() {
		return curator;
	}
	
	private void initPaths() {
		// Ensure base paths
		for (String path : Arrays.asList(CONFIG_BASE_PATH, configEnvironmentPath)) {
			ensurePath(path);
		}

		// Ensure types paths
		for (String type : configTypes.keySet()) {
			ensurePath(configEnvironmentPath + CONFIG_PATH_DELIMITER + type);
		}
	}
	
	private void ensurePath(String path) throws InvalidAccessException {
		try {
			if (curator.checkExists().forPath(path) == null) {
				logger.trace("ensurePath() path={} created", path); 
				curator.create().forPath(path);
			}
		} catch (Exception e) {
			throw new InvalidAccessException(e);
		}

	}
	
	public static class Builder {
		private final CuratorFramework curator;
		private final String environment;
		protected final Map<String, Class<Object>> configTypes = new HashMap<>();
		private final ISerializer serializer;

		public Builder(CuratorFramework curator, ISerializer serializer, String environment) {
			checkNotNull(curator);
			checkNotNull(serializer);
			checkArgument(curator.getState() == CuratorFrameworkState.STARTED);
			checkArgument(!Strings.isNullOrEmpty(environment));

			this.curator = curator;
			this.serializer = serializer;
			this.environment = environment;
		}

		@SuppressWarnings("unchecked")
		public Builder registerConfigType(String type, Class<?> typeClass) {
			configTypes.put(type, (Class<Object>)typeClass);
			return this;
		}

		
	}
}
