package com.ogp.configurator;

/**
 * @author Anton Kharenko
 */
public class TestConfiguration {
	
	private final String id;
	private final String name;
	private final String host;
	private final int port;

	public TestConfiguration(String id, String name, String host, int port) {
		this.id = id;
		this.name = name;
		this.host = host;
		this.port = port;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return "TestConfiguration{" +
				"id='" + id + '\'' +
				", name='" + name + '\'' +
				", host='" + host + '\'' +
				", port=" + port +
				'}';
	}
}
