package com.ogp.configurator;

import java.util.Objects;

/**
 * @author Anton Kharenko
 */
public class ConfigurationUpdateEvent {

	//TODO: Java Doc and consider to add generics

	public enum UpdateType {
		ADDED,
		REMOVED,
		UPDATED;
	}

	private final String type;

	private final Class typeClass;

	private final Object oldValue;

	private final Object newValue;

	private final UpdateType updateType;

	public ConfigurationUpdateEvent(String type, Class typeClass, Object oldValue, Object newValue, UpdateType updateType) {
		this.type = type;
		this.typeClass = typeClass;
		this.oldValue = oldValue;
		this.newValue = newValue;
		this.updateType = updateType;
	}

	public String getType() {
		return type;
	}

	public Class getTypeClass() {
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

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ConfigurationUpdateEvent that = (ConfigurationUpdateEvent) o;
		return Objects.equals(type, that.type) &&
				Objects.equals(typeClass, that.typeClass) &&
				Objects.equals(oldValue, that.oldValue) &&
				Objects.equals(newValue, that.newValue) &&
				Objects.equals(updateType, that.updateType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, typeClass, oldValue, newValue, updateType);
	}

	@Override
	public String toString() {
		return "ConfigurationUpdateEvent{" +
				"type='" + type + '\'' +
				", typeClass=" + typeClass +
				", oldValue=" + oldValue +
				", newValue=" + newValue +
				", updateType=" + updateType +
				'}';
	}
}
