package com.ogp.configurator;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Anton Kharenko
 */
public class ConfigService {

	private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);

	private static final String CONFIG_BASE_PATH = "/config";

	private final CuratorFramework zkClient;
	private final Map<String, Class> configTypes;
	private final Map<Class, String> classToType;
	private final String configEnvironmentPath;

	private final TreeCache configCache;

	public static Builder newBuilder(CuratorFramework zkClient, String environment) {
		return new Builder(zkClient, environment);
	}

	private ConfigService(Builder builder) {
		this.zkClient = builder.zkClient;
		this.configEnvironmentPath = CONFIG_BASE_PATH + "/" + builder.environment;
		this.configTypes = ImmutableMap.copyOf(builder.configTypes);

		// TODO: refactoring
		Map<Class, String> classToType = new HashMap<Class, String>(configTypes.size());
		for (Map.Entry<String, Class> typeEntry : configTypes.entrySet()) {
			classToType.put(typeEntry.getValue(), typeEntry.getKey());
		}
		this.classToType = ImmutableMap.copyOf(classToType);

		initPaths();

		// Init config cache
		// TODO: refactoring
		configCache = new TreeCache(zkClient, configEnvironmentPath);
		try {
			configCache.start();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
		configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				// TODO
				System.out.println("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
			}
		});
	}

	private static String childDataToString(ChildData childData) {
		if (childData == null) {
			return "null";
		} else {
			String dataAsString = childData.getData() == null  ? "null" : new String(childData.getData());
			return "path=" + childData.getPath() + " stat=" + childData.getStat() + " data=" + dataAsString;
		}
	}

	private void initPaths() {
		// Ensure base paths
		for (String path : Arrays.asList(CONFIG_BASE_PATH, configEnvironmentPath)) {
			ensurePath(path);
		}

		// Ensure types paths
		for (String type : configTypes.keySet()) {
			ensurePath(configEnvironmentPath + "/" + type);
		}
	}

	public <T> boolean upsertConfigEntity(String key, T config) {
		String type = classToType.get(config.getClass());
		return upsertConfigEntity(type, key, config.toString()); //TODO: deserialize object
	}

	public <T> boolean upsertConfigEntity(String type, String key, String config) {
		checkArgument(!Strings.isNullOrEmpty(type));
		checkArgument(!Strings.isNullOrEmpty(key));
		checkArgument(!Strings.isNullOrEmpty(config));

		final String configEntityPath = configEnvironmentPath + "/" + type + "/" + key;
		try {
			if (zkClient.checkExists().forPath(configEntityPath) == null) {
				zkClient.create().forPath(configEntityPath, config.getBytes());
			} else {
				zkClient.setData().forPath(configEntityPath, config.getBytes());
			}
			return true;
		} catch (Exception e) {
			logger.error("Failed to store at '{}' config: {}", configEntityPath, config, e);
			return false;
		}
	}

	public String getConfigEntity(Class clazz, String key) {
		String type = classToType.get(clazz);

		checkArgument(!Strings.isNullOrEmpty(type));
		checkArgument(!Strings.isNullOrEmpty(key));

		final String configEntityPath = configEntityPath(type, key);

		byte[] configData = new byte[0];
		try {
			configData = zkClient.getData().forPath(configEntityPath); //TODO: use local cache
		} catch (Exception e) {
			e.printStackTrace(); //TODO
		}

		return new String(configData);
	}

	public List<String> getAllConfigEntities(Class clazz) {
		String type = classToType.get(clazz);
		checkArgument(!Strings.isNullOrEmpty(type));

		Map<String, ChildData> entities = configCache.getCurrentChildren(configTypePath(type));
		List<String> configEntities = new ArrayList<String>(entities.size());
		for (ChildData cdata : entities.values()) {
			byte[] dataAsBytes = cdata.getData();
			if (dataAsBytes != null) {
				String dataAsString = new String(cdata.getData());
				configEntities.add(dataAsString); // TODO: deserialization
			}
		}

		return configEntities;
	}

	public void deleteConfigEntity(Class clazz, String key) {
		String type = classToType.get(clazz);
		checkArgument(!Strings.isNullOrEmpty(type));

		final String configEntityPath = configEntityPath(type, key);

		try {
			zkClient.delete().forPath(configEntityPath);
		} catch (Exception e) {
			e.printStackTrace(); // TODO
		}
	}

	private String configEntityPath(String type, String key) {
		return configTypePath(type) + "/" + key;
	}

	private String configTypePath(String type) {
		return configEnvironmentPath + "/" + type;
	}

	private void ensurePath(String path) {
		try {
			if (zkClient.checkExists().forPath(path) == null)
				zkClient.create().forPath(path);
		} catch (Exception e) {
			logger.error("Failed to ensure config path '{}'", path, e);
		}
	}

	public static class Builder {
		private final CuratorFramework zkClient;
		private final String environment;
		private final Map<String, Class> configTypes = new HashMap<String, Class>();

		public Builder(CuratorFramework zkClient, String environment) {
			checkNotNull(zkClient);
			checkArgument(zkClient.getState() == CuratorFrameworkState.STARTED);
			checkArgument(!Strings.isNullOrEmpty(environment));

			this.zkClient = zkClient;
			this.environment = environment;
		}

		public Builder registerConfigType(String type, Class typeClass) {
			configTypes.put(type, typeClass);
			return this;
		}

		public ConfigService build() {
			return new ConfigService(this);
		}
	}

}
