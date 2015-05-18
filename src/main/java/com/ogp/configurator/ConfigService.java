package com.ogp.configurator;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.ogp.configurator.serializer.ISerializer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Anton Kharenko
 */
public class ConfigService {

	private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);

	private static final String CONFIG_BASE_PATH = "/config";

	private final CuratorFramework zkClient;
	private final ISerializer serializer;
	private final Map<String, Class<Object>> configTypes;
	private final Map<Class<Object>, String> classToType;
	private final ConcurrentHashMap<Class<Object>, Map<String, Object>> configObjects;
	private final String configEnvironmentPath;
	private final TreeCache configCache;
	
	private boolean isInitialized; // switch to true after initialization complete, used during startup
	private boolean isReadOnly; // Indicate treeCache state, is it available for write.
	private final Object syncReady = new Object();

	public static Builder newBuilder(CuratorFramework zkClient, ISerializer serializer, String environment) {
		return new Builder(zkClient, serializer, environment);
	}

	private ConfigService(Builder builder) {
		this.isInitialized = false;
		this.isReadOnly = true;
		this.zkClient = builder.zkClient;
		this.serializer = builder.serializer;
		this.configEnvironmentPath = CONFIG_BASE_PATH + "/" + builder.environment;
		this.configTypes = ImmutableMap.copyOf(builder.configTypes);
		this.configObjects = new ConcurrentHashMap<Class<Object>, Map<String,Object>>(configTypes.size(), 0.9f, 1);

		// TODO: refactoring
		Map<Class<Object>, String> classToType = new HashMap<>(configTypes.size());
		for (Map.Entry<String, Class<Object>> typeEntry : configTypes.entrySet()) {
			classToType.put(typeEntry.getValue(), typeEntry.getKey());
			configObjects.putIfAbsent(typeEntry.getValue(), new HashMap<String, Object>());
		}
		this.classToType = ImmutableMap.copyOf(classToType);

		initPaths();

		configCache = new TreeCache(zkClient, configEnvironmentPath);
		try {
			configCache.start();
		} catch (Exception e) {
			logger.error("Failed to TreeCache", e);
			throw new RuntimeException();
		}
		
		configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				logger.debug("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
				processEvent(event);
			}
		});
	}

	private void processEvent(TreeCacheEvent event) throws Exception {
		Class<Object> configEntityClass = childDataToClass(event.getData());
		switch (event.getType()) {
			case NODE_ADDED:
			case NODE_UPDATED:
				if (configEntityClass != null 
								&& isInitialized) {
					Object obj = serializer.Deserialize(event.getData().getData(), configEntityClass);
					String key = childDataToKey(event.getData());
					Map<String, Object> entities = configObjects.putIfAbsent(configEntityClass, new HashMap<String, Object>());
					synchronized (entities) {
						entities.put(key, obj);
					}							
					logger.trace("key=%s put/update in cache, now %d classes and %d objects of this key in cache\n", 
							key,
							configObjects.size(),
							configObjects.get(configEntityClass).size());
				}
				break;
			case NODE_REMOVED:
				if (configEntityClass != null
								&& isInitialized) {
					String key = childDataToKey(event.getData());
					if (configObjects.containsKey(configEntityClass)) {
						Map<String, Object> entities = configObjects.get(configEntityClass);
						synchronized (entities) {
							entities.remove(key);
						}
					}
				}
				break;
			case INITIALIZED:
				reloadConfigTree();
				logger.info("Initialization complete");
				isInitialized = true;
				isReadOnly = false;
				synchronized (syncReady) {
					syncReady.notify();
				}
				break;
			case CONNECTION_LOST:
			case CONNECTION_SUSPENDED:
				isReadOnly = true;
				logger.info("Connection with ZooKeeper lost");
				break;
			case CONNECTION_RECONNECTED:
				isReadOnly = false;
				synchronized (syncReady) {
					syncReady.notify();
				}
				logger.info("Connection with ZooKeeper restored");
				break;
		}		
	}
	
	private Class<Object> childDataToClass(ChildData childData) {
		if (childData == null) {
			return null;
		}
		String path = childData.getPath();
		String[] pathElements = path.split("/");
		if (pathElements.length > 1) {
			String type = pathElements[pathElements.length-2];
			return configTypes.get(type);
		}
		return null;
	}
	
	private String childDataToKey(ChildData childData) {
		if (childData == null) {
			return null;
		}
		String path = childData.getPath();
		String[] pathElements = path.split("/");
		if (pathElements.length > 1) {
			String type = pathElements[pathElements.length-1];
			return type;
		}
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

	public void BlockUntilReady() {
		synchronized (syncReady) {
			while (!(isInitialized && !isReadOnly)) {
				try {
					syncReady.wait();
				} catch (InterruptedException e) {
					
				}
			}
		}
	}
	
	public <T> boolean upsertConfigEntity(String key, T config) throws Exception {
		String type = classToType.get(config.getClass());
		Class<Object> clazz = configTypes.get(type);
		if (upsertConfigEntity(type, key, serializer.Serialize(config))) {
			Map<String, Object> entities = configObjects.putIfAbsent(clazz, new HashMap<String, Object>());
			synchronized (entities) {
				entities.put(key, config);
			}							
			logger.trace("key=%s put/update in cache, now %d classes and %d objects of this key in cache\n", 
					key,
					configObjects.size(),
					configObjects.get(clazz).size());
			return true;
		}
		return false;
	}

	private boolean upsertConfigEntity(String type, String key, byte[] config) {
		checkArgument(!Strings.isNullOrEmpty(type));
		checkArgument(!Strings.isNullOrEmpty(key));
		checkArgument(config.length > 0);
		
		if (!(isInitialized && !isReadOnly)) {
			return false;
		}
		
		final String configEntityPath = configEnvironmentPath + "/" + type + "/" + key;
		try {
			if (zkClient.checkExists().forPath(configEntityPath) == null) {
				zkClient.create().forPath(configEntityPath, config);
			} else {
				zkClient.setData().forPath(configEntityPath, config);
			}
			logger.trace("upsertConfigEntity() add new ConfigEntity type,key=("+type+","+key+");");
			return true;
		} catch (Exception e) {
			logger.error("Failed to store at '{}' config: {}", configEntityPath, config, e);
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T getConfigEntity(Class<T> clazz, String key) {
		String type = classToType.get(clazz);

		checkArgument(!Strings.isNullOrEmpty(type));
		checkArgument(!Strings.isNullOrEmpty(key));
		
		//Check existent
		if (configObjects.containsKey(clazz)) {
			Map<String, Object> entities = configObjects.get(clazz);
			synchronized (entities) {
				return (T)entities.get(key);
			}
		}
		return null;
	}

	public List<Object> getAllConfigEntities(Class<? extends Object> clazz) {
		String type = classToType.get(clazz);
		Class<Object> classFromType = configTypes.get(type);
		checkArgument(!Strings.isNullOrEmpty(type));
		checkArgument(classFromType != null);
		
		final List<Object> configEntities = new ArrayList<>();
		
		if (configObjects.containsKey(classFromType)) {
			logger.debug("Config cache for class %s have %d elements\n", classFromType.toString(), configObjects.get(classFromType).size());
			Map<String, Object> entities = configObjects.get(classFromType);
			synchronized (entities) {
				configEntities.addAll(entities.values());
			}
		} else {
			logger.debug("Class %s not found in cache\n", classFromType);
		}
		return configEntities;
	}
	
	public <T> void deleteConfigEntity(Class<T> clazz, String key) {
		String type = classToType.get(clazz);
		checkArgument(!Strings.isNullOrEmpty(type));
		
		if (!isInitialized) {
			return;
		}
		
		final String configEntityPath = configEntityPath(type, key);

		try {
			zkClient.delete().forPath(configEntityPath);
			if (configObjects.containsKey(clazz)) {
				Map<String, Object> entities = configObjects.get(clazz);
				synchronized (entities) {
					entities.remove(key);
				}
			}
		} catch (Exception e) {
			logger.error("Failed to delete at '{}'", configEntityPath, e);
		}
	}

	private void reloadConfigTree() {
		for (Class<Object> clazz : classToType.keySet()) {
			String path = configTypePathForClass(clazz);
			//Cleanup all entities for every 
			Map<String, Object> entities = configObjects.putIfAbsent(clazz, new HashMap<String, Object>());
			synchronized (entities) {
				entities.clear();
			}
			Map<String,ChildData> treeCacheObjects = configCache.getCurrentChildren(path);
			if (treeCacheObjects != null) {
				for (String  key : treeCacheObjects.keySet()) {
					ChildData data = treeCacheObjects.get(key);
					Object obj;
					try {
						obj = serializer.Deserialize(data.getData(), clazz);
						synchronized (entities) {
							entities.put(key, obj);
						}							
						logger.debug("reloadConfigTree() key=%s put in cache, now %d classes and %d objects\n", 
								key,
								configObjects.size(),
								configObjects.get(clazz).size());
					} catch (Exception e) {
						logger.error("Failed to deserialize at path '{}', key %s", path, key, e);
					}
				}
			}
		}
	}
	private String configEntityPath(String type, String key) {
		return configTypePath(type) + "/" + key;
	}

	private String configTypePath(String type) {
		return configEnvironmentPath + "/" + type;
	}

	private String configTypePathForClass(Class<? extends Object> clazz) {
		return configEnvironmentPath + "/" + classToType.get(clazz);
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
		private final Map<String, Class<Object>> configTypes = new HashMap<>();
		private final ISerializer serializer;

		public Builder(CuratorFramework zkClient, ISerializer serializer, String environment) {
			checkNotNull(zkClient);
			checkNotNull(serializer);
			checkArgument(zkClient.getState() == CuratorFrameworkState.STARTED);
			checkArgument(!Strings.isNullOrEmpty(environment));

			this.zkClient = zkClient;
			this.serializer = serializer;
			this.environment = environment;
		}

		@SuppressWarnings("unchecked")
		public Builder registerConfigType(String type, Class<? extends Object> typeClass) {
			configTypes.put(type, (Class<Object>)typeClass);
			return this;
		}

		public ConfigService build() {
			return new ConfigService(this);
		}
	}

}
