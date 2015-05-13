package com.ogp.configurator;

/**
 * @author Anton Kharenko
 */
public class ServerConfigEntity {

	private  String id;
	private  String name;
	private  String host;
	private  int port;

	public ServerConfigEntity() {
		
	}
	
	public ServerConfigEntity(String id, String name, String host, int port) {
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

	
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	
	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	
	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
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
