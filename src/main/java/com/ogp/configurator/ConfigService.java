package com.ogp.configurator;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.ogp.configurator.ConfigurationEvent.ConfigType;
import com.ogp.configurator.ConfigurationEvent.UpdateType;
import com.ogp.configurator.serializer.ISerializer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Anton Kharenko
 */
public class ConfigService implements IConfigurationManagement {

	private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);

	private static final String CONFIG_BASE_PATH = "/config";

	private final CuratorFramework zkClient;
	private final ISerializer serializer;
	private final Map<String, Class<Object>> configTypes;
	private final Map<Class<Object>, String> classToType;
	private final ConcurrentHashMap<Class<Object>, Map<String, Object>> configObjects;
	private final String configEnvironmentPath;
	private final TreeCache configCache;
	
	private final PublishSubject<ConfigurationEvent> subject;
	
	private volatile boolean isInitialized; // switch to true after initialization complete, used during startup
	private volatile boolean isReadOnly; // Indicate treeCache state, is it available for write.
	
	private volatile int connectionPhase;
	private Phaser connectionPhaser;
	

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
		this.configObjects = new ConcurrentHashMap<>(configTypes.size(), 0.9f, 1);
		this.subject = PublishSubject.create();
		//this.syncInitialization = new CountDownLatch(1);
		this.connectionPhase = 0;
		this.connectionPhaser = new Phaser(1);
		
		// TODO: refactoring
		Map<Class<Object>, String> classToType = new HashMap<>(configTypes.size());
		for (Map.Entry<String, Class<Object>> typeEntry : configTypes.entrySet()) {
			classToType.put(typeEntry.getValue(), typeEntry.getKey());
			configObjects.putIfAbsent(typeEntry.getValue(), new HashMap<String, Object>());
		}
		this.classToType = ImmutableMap.copyOf(classToType);

		initPaths();

		configCache = new TreeCache(zkClient, configEnvironmentPath);
		
		configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				logger.debug("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
				processEvent(event);
			}
		});
	}

	private void processEvent(TreeCacheEvent event) throws Exception {
		Class<Object> configEntityClass = childDataToClass(event.getData());
		boolean isAdd = false;
		switch (event.getType()) {
			case NODE_ADDED:
				isAdd = true;
			case NODE_UPDATED:
				if (configEntityClass != null) {
					Object newObj = serializer.deserialize(event.getData().getData(), configEntityClass);
					String key = childDataToKey(event.getData());
					Object updated = null;
					Map<String, Object> entities = configObjects.putIfAbsent(configEntityClass, new HashMap<String, Object>());
					synchronized (entities) {
						updated = entities.put(key, newObj);
					}				
					if (isAdd) {
						subject.onNext(new ConfigurationEvent(key, configEntityClass, updated, newObj, UpdateType.ADDED));
					} else {
						subject.onNext(new ConfigurationEvent(key, configEntityClass, updated, newObj, UpdateType.UPDATED));
					}
					logger.trace("key={} put/update in cache, now {} classes and {} objects of this key in cache\n", 
							key,
							configObjects.size(),
							configObjects.get(configEntityClass).size());
				}
				break;
			case NODE_REMOVED:
				if (configEntityClass != null) {
					String key = childDataToKey(event.getData());
					if (configObjects.containsKey(configEntityClass)) {
						Object removed;
						Map<String, Object> entities = configObjects.get(configEntityClass);
						synchronized (entities) {
							removed = entities.remove(key);
						}
						subject.onNext(new ConfigurationEvent(key, configEntityClass, removed, null, UpdateType.REMOVED));
					}
				}
				break;
			case INITIALIZED:
				logger.info("Initialization complete");
				subject.onNext(new ConfigurationEvent(ConfigType.INITIALIZED));
				isInitialized = true;
				break;
			case CONNECTION_LOST:
			case CONNECTION_SUSPENDED:
				isReadOnly = true;
				if (connectionPhase == connectionPhaser.getPhase())
					connectionPhase++;
				logger.info("Connection with ZooKeeper lost");
				subject.onNext(new ConfigurationEvent(ConfigType.CONNECTION_LOST));
				break;
			case CONNECTION_RECONNECTED:
				isReadOnly = false;
				connectionPhaser.arrive();
				logger.info("Connection with ZooKeeper restored");
				subject.onNext(new ConfigurationEvent(ConfigType.CONNECTION_RESORED));
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

	@Override
	public void start() throws ConnectionLossException {
		try {
			configCache.start();
			isReadOnly = false;
			connectionPhaser.arrive();
		} catch (Exception e) {
			logger.error("Failed to TreeCache", e);
			throw new ConnectionLossException(e);
		}
		
	}

	
	@Override
	public void awaitConnected() throws InterruptedException {
		connectionPhaser.awaitAdvanceInterruptibly(connectionPhase);
	}
	
	@Override
	public boolean awaitConnected(long timeout, TimeUnit unit) throws InterruptedException {
		try {
			connectionPhaser.awaitAdvanceInterruptibly(connectionPhase, timeout, unit);
			return true;
		} catch(TimeoutException te) {
			return false;
		}
	}
	
	@Override
	public boolean isConnected() {
		return !isReadOnly;
	}
	
	@Override
	public boolean isInitialized() {
		return isInitialized;
	}
	
	@Override
	public <T> void save(String key, T config) throws ConnectionLossException {
		if (config == null)
			throw new UnknownTypeException("config is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!isConnected()) {
			throw new ConnectionLossException("Config service not connected to ZooKeeper");
		}
		
		if (!classToType.containsKey(config.getClass()))
			throw new UnknownTypeException("Specified "+config.getClass().toString()+" not registred");
		
		String type = classToType.get(config.getClass());
		Class<Object> clazz = configTypes.get(type);
		if (saveConfigEntity(type, key, serializer.serialize(config))) {
			Map<String, Object> entities = configObjects.putIfAbsent(clazz, new HashMap<String, Object>());
			synchronized (entities) {
				entities.put(key, config);
			}							
			logger.trace("key={} put/update in cache, now {} classes and {} objects of this key in cache\n", 
					key,
					configObjects.size(),
					configObjects.get(clazz).size());
		}
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> clazz, String key) {
		if (clazz == null)
			throw new UnknownTypeException("clazz is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!classToType.containsKey(clazz))
			throw new UnknownTypeException("Specified "+clazz.toString()+" not registred");

		//Check existent
		if (configObjects.containsKey(clazz)) {
			Map<String, Object> entities = configObjects.get(clazz);
			synchronized (entities) {
				return (T)entities.get(key);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> list(Class<T> type) {
		if (type == null)
			throw new UnknownTypeException("type is null");
		
		if (!classToType.containsKey(type))
			throw new UnknownTypeException("Specified "+type.toString()+" not registred");
		
		String typeString = classToType.get(type);
		
		if (!configTypes.containsKey(typeString))
			throw new UnknownTypeException("Missconfiguration: Specified "+type.toString()+" registred but not found in configTypes map");
		
		Class<Object> classFromType = configTypes.get(typeString);

		final List<T> configEntities = new ArrayList<>();
		
		if (configObjects.containsKey(classFromType)) {
			logger.debug("Config cache for class {} have {} elements\n", classFromType.toString(), configObjects.get(classFromType).size());
			Map<String, Object> entities = configObjects.get(classFromType);
			synchronized (entities) {
				configEntities.addAll((Collection<T>) entities.values());
			}
		} else {
			logger.debug("Class {} not found in cache\n", classFromType);
		}
		return configEntities;
		
	}
	
	@Override
	public <T> void delete(Class<T> clazz, String key) throws ConnectionLossException {
		if (clazz == null)
			throw new UnknownTypeException("clazz is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!isConnected()) {
			throw new ConnectionLossException("Config service not connected to ZooKeeper");
		}
		
		if (!classToType.containsKey(clazz))
			throw new UnknownTypeException("Specified "+clazz.toString()+" not registred");
		
		String type = classToType.get(clazz);
		
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
			throw new InvalidAccessException(e);
		}
	}

	@Override
	public Observable<ConfigurationEvent> listenUpdates() {
		return subject;
	}

	
	private boolean saveConfigEntity(String type, String key, byte[] config) {
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
			throw new InvalidAccessException(e);
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
		public Builder registerConfigType(String type, Class<?> typeClass) {
			configTypes.put(type, (Class<Object>)typeClass);
			return this;
		}

		public ConfigService build() {
			return new ConfigService(this);
		}
	}

	

	

	

	


	


	

	

}
