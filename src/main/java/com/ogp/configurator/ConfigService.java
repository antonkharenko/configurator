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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Anton Kharenko
 */
public class ConfigService {

	private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);

	private static final String CONFIG_BASE_PATH = "/config";

	private final CuratorFramework zkClient;
	private final Map<String, Class<Object>> configTypes;
	private final Map<Class<Object>, String> classToType;
	private final ConcurrentHashMap<Class<Object>, Map<String, Object>> configObjects;
	private final String configEnvironmentPath;
	private final TreeCache configCache;
	
	private boolean isInitialized; // switch to true after initialization complete, used during startup
	private boolean isReadOnly; // Indicate treeCache state, is it available for write.

	public static Builder newBuilder(CuratorFramework zkClient, String environment) {
		return new Builder(zkClient, environment);
	}

	private ConfigService(Builder builder) {
		this.isInitialized = false;
		this.isReadOnly = true;
		this.zkClient = builder.zkClient;
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
			e.printStackTrace();
			throw new RuntimeException();
		}
		
		configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				System.out.println("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
				Class<Object> configEntityClass = childDataToClass(event.getData());
				switch (event.getType()) {
					case NODE_ADDED:
					case NODE_UPDATED:
						if (configEntityClass != null 
										&& isInitialized) {
							Object obj = JacksonSerializator.Deserialize(event.getData().getData(), configEntityClass);
							String key = childDataToKey(event.getData());
							Map<String, Object> entities = configObjects.putIfAbsent(configEntityClass, new HashMap<String, Object>());
							synchronized (entities) {
								entities.put(key, obj);
							}							
							System.out.printf("childEvent() key=%s put in cache, now %d classes and %d objects\n", 
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
						System.out.println("INITIALIZED, COMPLETE");
						isInitialized = true;
						isReadOnly = false;
						break;
					case CONNECTION_LOST:
					case CONNECTION_SUSPENDED:
						isReadOnly = true;
						break;
					case CONNECTION_RECONNECTED:
						isReadOnly = false;
						break;
				}
			}
		});
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

	public <T> boolean upsertConfigEntity(String key, T config) throws Exception {
		String type = classToType.get(config.getClass());
		return upsertConfigEntity(type, key, JacksonSerializator.Serialize(config));
	}

	public boolean upsertConfigEntity(String type, String key, byte[] config) {
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
			System.out.println("upsertConfigEntity() add new ConfigEntity type,key=("+type+","+key+");");
			return true;
		} catch (Exception e) {
			logger.error("Failed to store at '{}' config: {}", configEntityPath, config, e);
			return false;
		}
	}

	public Object getConfigEntity(Class<Object> clazz, String key) {
		String type = classToType.get(clazz);

		checkArgument(!Strings.isNullOrEmpty(type));
		checkArgument(!Strings.isNullOrEmpty(key));
		
		//Check existent
		if (configObjects.containsKey(clazz)) {
			Map<String, Object> entities = configObjects.get(clazz);
			synchronized (entities) {
				return entities.get(key);
			}
		}

		final String configEntityPath = configEntityPath(type, key);

		byte[] configData = new byte[0];
		try {
			configData = zkClient.getData().forPath(configEntityPath);
			Object obj = JacksonSerializator.Deserialize(configData, clazz);
			Map<String, Object> entities = configObjects.putIfAbsent(clazz, new HashMap<String, Object>());
			synchronized (entities) {
				entities.put(key, obj);
			}
			return obj;
		} catch (Exception e) {
			e.printStackTrace(); //TODO
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
			System.out.printf("Config cache for class %s have %d elements\n", classFromType.toString(), configObjects.get(classFromType).size());
			Map<String, Object> entities = configObjects.get(classFromType);
			synchronized (entities) {
				configEntities.addAll(entities.values());
			}
		} else {
			System.out.printf("Class %s not found in cache\n", classFromType);
		}
		return configEntities;
	}
	
	public void deleteConfigEntity(Class<Object> clazz, String key) {
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
			e.printStackTrace(); // TODO
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
						obj = JacksonSerializator.Deserialize(data.getData(), clazz);
						synchronized (entities) {
							entities.put(key, obj);
						}							
						System.out.printf("reloadConfigTree() key=%s put in cache, now %d classes and %d objects\n", 
								key,
								configObjects.size(),
								configObjects.get(clazz).size());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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

		public Builder(CuratorFramework zkClient, String environment) {
			checkNotNull(zkClient);
			checkArgument(zkClient.getState() == CuratorFrameworkState.STARTED);
			checkArgument(!Strings.isNullOrEmpty(environment));

			this.zkClient = zkClient;
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
