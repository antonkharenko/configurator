package com.ogp.configurator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import com.ogp.configurator.ConfigurationEvent.ConfigType;
import com.ogp.configurator.ConfigurationEvent.UpdateType;
import com.ogp.configurator.serializer.ISerializer;

/**
* IConfiguration interface implementation class.
* Provides get(), list() methods for access to Cache of configuration objects
* and Observable interface to object updates events.
*
* @author Andriy Panasenko
*/
public class Configuration extends ConfigurationMonitor implements IConfiguration {

	private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
	private final ConcurrentHashMap<Class<? extends Object>, Map<String, Object>> configObjects;
	private final CountDownLatch syncInit;
	private final PublishSubject<ConfigurationEvent> subject;
	
	
	private Configuration(Builder builder) {
		super(builder);
		this.syncInit = new CountDownLatch(1);
		this.configObjects = new ConcurrentHashMap<>(builder.configTypes.size(), 0.9f, 1);
		this.subject = PublishSubject.create();
		
		OnConnection onConnection = new OnConnection();
		super.listen().filter(onConnection).subscribe(onConnection);
		OnInitialized onInitialized = new OnInitialized();
		super.listen().filter(onInitialized).subscribe(onInitialized);
		OnAddOrUpdate onAddOrUpdate = new OnAddOrUpdate();
		super.listen().filter(onAddOrUpdate).subscribe(onAddOrUpdate);
		OnRemoved onRemoved = new OnRemoved();
		super.listen().filter(onRemoved).subscribe(onRemoved);
		
		super.listen().doOnError(new Action1<Throwable>() {
			@Override
			public void call(Throwable t1) {
				subject.onError(t1);
			}
		});
		super.listen().doOnCompleted(new Action0() {
			@Override
			public void call() {
				subject.onCompleted();
			}
		});
		
		
//		super.listen().subscribe(new Observer<ConfigurationEvent>() {
//
//			@Override
//			public void onCompleted() {
//				logger.trace("Configuration Event onCompleted() push event on subject()");
//				subject.onCompleted();
//			}
//
//			@Override
//			public void onError(Throwable e) {
//				logger.trace("Configuration Event onError() push event on subject(), error: {}", e.toString());
//				subject.onError(e);
//			}
//
//			@Override
//			public void onNext(ConfigurationEvent event) {
//				logger.info("Configuration Event onNext() ConfigType={}, updateType={}, class={}",
//						event.getConfigType(),
//						event.getUpdateType(),
//						event.getTypeClass().toString());
//				if (event.getConfigType() == ConfigType.INITIALIZED) {
//					logger.info("Configuration() initialization Done.");
//					syncInit.countDown();
//				}
//				Map<String, Object> entities;
//				switch (event.getUpdateType()) {
//				case ADDED:
//				case UPDATED:
//					Object updated = null;
//					entities = configObjects.putIfAbsent(event.getTypeClass(), new HashMap<String, Object>());
//					if (entities == null) {
//						entities = configObjects.get(event.getTypeClass());
//					}
//					synchronized (entities) {
//						updated = entities.put(event.getKey(), event.getNewValue());
//					}
//					if (updated != null) {
//						event.setOldValue(updated);
//						logger.trace("Configuration Event onNext() updateType={}, key={}, class={}, newValue={}, oldValue={}",
//								event.getUpdateType(),
//								event.getKey(),
//								event.getTypeClass().toString(),
//								event.getNewValue().toString(),
//								event.getOldValue().toString());
//					} else {
//						logger.trace("Configuration Event onNext() updateType={}, key={}, class={}, newValue={}",
//								event.getUpdateType(),
//								event.getKey(),
//								event.getTypeClass().toString(),
//								event.getNewValue().toString());
//					}
//					break;
//				case REMOVED:
//					if (configObjects.containsKey(event.getTypeClass())) {
//						Object removed;
//						entities = configObjects.get(event.getTypeClass());
//						if (entities != null) {
//							synchronized (entities) {
//								removed = entities.remove(event.getKey());
//							}
//							event.setOldValue(removed);
//							logger.trace("Configuration Event onNext() updateType={}, key={}, class={}, oldValue={}",
//									event.getUpdateType(),
//									event.getKey(),
//									event.getTypeClass().toString(),
//									event.getOldValue().toString());
//						}
//					}
//					break;
//				default:
//					break;
//				}
//				logger.trace("Configuration Event onNext() push event on subject()");
//				subject.onNext(event);
//			}
//		});
//		
		
	}
	
	public static Builder newBuilder(CuratorFramework zkClient, ISerializer serializer, String environment) {
		return new Builder(zkClient, serializer, environment);
	}
	
	@Override
	public void start() throws ConnectionLossException {
		super.start();
	}

	@Override
	public void awaitInitialized() throws InterruptedException {
		syncInit.await();
	}

	@Override
	public boolean awaitInitialized(long timeout, TimeUnit unit) throws InterruptedException {
		return syncInit.await(timeout, unit);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> type, String key) {
		if (type == null)
			throw new UnknownTypeException("clazz is null");
		if (key == null)
			throw new UnknownTypeException("key is null");
		
		if (!isTypeConfigured(type))
			throw new UnknownTypeException("Specified "+type.toString()+" not registred");

		//Check existent
		if (configObjects.containsKey(type)) {
			Map<String, Object> entities = configObjects.get(type);
			synchronized (entities) {
				return (T)entities.get(key);
			}
		} else {
			logger.debug("Class {} not found in cache", type);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> list(Class<T> type) {
		if (type == null)
			throw new UnknownTypeException("type is null");
		
		if (!isTypeConfigured(type))
			throw new UnknownTypeException("Specified "+type.toString()+" not registred");
		
		final List<T> configEntities = new ArrayList<>();
		
		if (configObjects.containsKey(type)) {
			logger.trace("Config cache for class {} have {} elements\n", type.toString(), configObjects.get(type).size());
			Map<String, Object> entities = configObjects.get(type);
			synchronized (entities) {
				configEntities.addAll((Collection<T>) entities.values());
			}
		} else {
			logger.debug("Class {} not found in cache", type);
		}
		return configEntities;
	}
	
	@Override
	public Observable<ConfigurationEvent> listen() {
		return this.subject;
	}
	
	public static class Builder extends ConfigurationMonitor.Builder {

		public Builder(CuratorFramework zkClient, ISerializer serializer, String environment) {
			super(zkClient, serializer, environment);
		}

		public Builder registerConfigType(String type, Class<?> typeClass) {
			super.registerConfigType(type, typeClass);
			return this;
		}
		
		public Configuration build() {
			return new Configuration(this);
		}
	}
	
	private abstract class DefaultObserver 
		implements Observer<ConfigurationEvent>,
					Func1<ConfigurationEvent, Boolean> {
	
		@Override
		public void onCompleted() {}
	
		@Override
		public void onError(Throwable e) {}
		
	}
	
	private class OnConnection extends DefaultObserver {
	
		@Override
		public Boolean call(ConfigurationEvent event) {
			if (event.getConfigType() == ConfigType.CONNECTION_LOST
					|| event.getConfigType() == ConfigType.CONNECTION_RESTORED)
				return true;
			else
				return false;
		}
		
		@Override
		public void onNext(ConfigurationEvent event) {
			logger.trace("OnConnection event {} passed.", event.toString());
			subject.onNext(event);
		}
	}
	
	private class OnInitialized extends DefaultObserver {
	
		@Override
		public Boolean call(ConfigurationEvent event) {
			if (event.getConfigType() == ConfigType.INITIALIZED)
				return true;
			else
				return false;
		}
		
		@Override
		public void onNext(ConfigurationEvent event) {
			logger.info("Configuration() initialization Done.");
			syncInit.countDown();
			subject.onNext(event);
		}
	}
	
	private class OnAddOrUpdate extends DefaultObserver {
	
		@Override
		public Boolean call(ConfigurationEvent event) {
			if (event.getUpdateType() == UpdateType.ADDED
					|| event.getUpdateType() == UpdateType.UPDATED)
				return true;
			else
				return false;
		}
		
		@Override
		public void onNext(ConfigurationEvent event) {
			logger.trace("Configuration OnAddOrUpdate Event onNext() ConfigType={}, updateType={}, class={}",
					event.getConfigType(),
					event.getUpdateType(),
					event.getTypeClass().toString());
			Map<String, Object> entities;
			Object updated = null;
			entities = configObjects.putIfAbsent(event.getTypeClass(), new HashMap<String, Object>());
			if (entities == null) {
				entities = configObjects.get(event.getTypeClass());
			}
			synchronized (entities) {
				updated = entities.put(event.getKey(), event.getNewValue());
			}
			if (updated != null) {
				event.setOldValue(updated);
				logger.trace("Configuration OnAddOrUpdate Event onNext() updateType={}, key={}, class={}, newValue={}, oldValue={}",
						event.getUpdateType(),
						event.getKey(),
						event.getTypeClass().toString(),
						event.getNewValue().toString(),
						event.getOldValue().toString());
			} else {
				logger.trace("Configuration OnAddOrUpdate Event onNext() updateType={}, key={}, class={}, newValue={}",
						event.getUpdateType(),
						event.getKey(),
						event.getTypeClass().toString(),
						event.getNewValue().toString());
			}
			subject.onNext(event);
		}
	}
	
	private class OnRemoved extends DefaultObserver {
	
		@Override
		public Boolean call(ConfigurationEvent event) {
			if (event.getUpdateType() == UpdateType.REMOVED)
				return true;
			else
				return false;
		}
		
		@Override
		public void onNext(ConfigurationEvent event) {
			if (configObjects.containsKey(event.getTypeClass())) {
				Object removed;
				Map<String, Object> entities = configObjects.get(event.getTypeClass());
				if (entities != null) {
					synchronized (entities) {
						removed = entities.remove(event.getKey());
					}
					event.setOldValue(removed);
					logger.trace("Configuration OnRemoved Event onNext() updateType={}, key={}, class={}, oldValue={}",
							event.getUpdateType(),
							event.getKey(),
							event.getTypeClass().toString(),
							event.getOldValue().toString());
				} else {
					logger.debug("Configuration OnRemoved Event onNext() Class {} not found in cache", event.getTypeClass().toString());
				}
			}
			subject.onNext(event);
		}
	}
	

}
