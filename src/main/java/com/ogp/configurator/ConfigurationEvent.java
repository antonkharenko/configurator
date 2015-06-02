package com.ogp.configurator;

import java.util.Objects;

/**
 * @author Anton Kharenko
 */
public class ConfigurationEvent {

	//TODO: Java Doc and consider to add generics
	public static final String DEFAULT_TYPE = "unused";
	public static final Class<ConfigurationEvent> DEFAULT_CLASS = ConfigurationEvent.class;

	public enum UpdateType {
		UNDEFINED,
		ADDED,
		REMOVED,
		UPDATED;
	}
	
	public enum ConfigType {
		UNDEFINED,
		INITIALIZED,
		CONNECTION_RESTORED,
		CONNECTION_LOST;
	}

	private final String type;

	private final Class<?> typeClass;
	
	private final String key;

	private Object oldValue;

	private final Object newValue;

	private final UpdateType updateType;
	
	private final ConfigType configType;

	public ConfigurationEvent(ConfigType configType) {
		this.configType = configType;
		this.type = DEFAULT_TYPE;
		this.typeClass = DEFAULT_CLASS;
		this.oldValue = null;
		this.newValue = null;
		this.key = new String("");
		this.updateType = UpdateType.UNDEFINED;
	}
	
	public ConfigurationEvent(String type, Class<?> typeClass, String key, Object oldValue, Object newValue, UpdateType updateType) {
		this.type = type;
		this.typeClass = typeClass;
		this.oldValue = oldValue;
		this.newValue = newValue;
		this.key = key;
		this.updateType = updateType;
		this.configType = ConfigType.UNDEFINED;
	}

	public String getType() {
		return type;
	}

	public Class<?> getTypeClass() {
		return typeClass;
	}

	public String getKey() {
		return key;
	}
	
	public Object getOldValue() {
		return oldValue;
	}
	
	public void setOldValue(Object obj) {
		oldValue = obj;
	}
	
	public Object getNewValue() {
		return newValue;
	}
	
	

	public UpdateType getUpdateType() {
		return updateType;
	}
	
	/**
	 * @return the configType
	 */
	public ConfigType getConfigType() {
		return configType;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ConfigurationEvent that = (ConfigurationEvent) o;
		return Objects.equals(type, that.type) &&
				Objects.equals(typeClass, that.typeClass) &&
				Objects.equals(key, that.key) &&
				Objects.equals(oldValue, that.oldValue) &&
				Objects.equals(newValue, that.newValue) &&
				Objects.equals(updateType, that.updateType) &&
				Objects.equals(configType, that.configType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, key, typeClass, oldValue, newValue, updateType, configType);
	}

	@Override
	public String toString() {
		return "ConfigurationEvent{" +
				"type='" + type + '\'' +
				", typeClass=" + typeClass +
				", key=" + key +
				", oldValue=" + oldValue +
				", newValue=" + newValue +
				", updateType=" + updateType +
				", configType=" + configType +
				'}';
	}

	
}
