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
		CONNECTION_RESORED,
		CONNECTION_LOST;
	}

	private final String type;

	private final Class<?> typeClass;

	private final Object oldValue;

	private final Object newValue;

	private final UpdateType updateType;
	
	private final ConfigType configType;

	public ConfigurationEvent(ConfigType configType) {
		this.configType = configType;
		this.type = DEFAULT_TYPE;
		this.typeClass = DEFAULT_CLASS;
		this.oldValue = null;
		this.newValue = null;
		this.updateType = UpdateType.UNDEFINED;
	}
	
	public ConfigurationEvent(String type, Class<?> typeClass, Object oldValue, Object newValue, UpdateType updateType) {
		this.type = type;
		this.typeClass = typeClass;
		this.oldValue = oldValue;
		this.newValue = newValue;
		this.updateType = updateType;
		this.configType = ConfigType.UNDEFINED;
	}

	public String getType() {
		return type;
	}

	public Class<?> getTypeClass() {
		return typeClass;
	}

	public Object getOldValue() {
		return oldValue;
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
				Objects.equals(oldValue, that.oldValue) &&
				Objects.equals(newValue, that.newValue) &&
				Objects.equals(updateType, that.updateType) &&
				Objects.equals(configType, that.configType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, typeClass, oldValue, newValue, updateType, configType);
	}

	@Override
	public String toString() {
		return "ConfigurationEvent{" +
				"type='" + type + '\'' +
				", typeClass=" + typeClass +
				", oldValue=" + oldValue +
				", newValue=" + newValue +
				", updateType=" + updateType +
				", configType=" + configType +
				'}';
	}

	
}
