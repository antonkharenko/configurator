package com.ogp.configurator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

public class App {

	public static void main(String[] args) throws Exception {
		// Start ZK client
		String zookeeperConnectionString = "127.0.0.1:2181";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();

		// Init config path
		String configPath = "/config/dev/TestConfiguration";
		if (client.checkExists().forPath("/config") == null)
			client.create().forPath("/config");
		if (client.checkExists().forPath("/config/dev") == null)
			client.create().forPath("/config/dev");
		if (client.checkExists().forPath("/config/dev/TestConfiguration") == null)
			client.create().forPath("/config/dev/TestConfiguration");

		// Set config entity data
		TestConfiguration testConfiguration = new TestConfiguration("123", "test-server", "127.0.0.1", 3456);
		String configDataPath = configPath + testConfiguration.getId();
		if (client.checkExists().forPath(configDataPath) == null)
			client.create().forPath(configDataPath, testConfiguration.toString().getBytes());
		else
			client.setData().forPath(configDataPath, testConfiguration.toString().getBytes());

		// Read config entity data
		byte[] configData = client.getData().forPath(configDataPath);
		String configDataAsString = new String(configData);
		System.out.println(configDataAsString);

		// List config entities
		List<String> entities = client.getChildren().forPath(configPath);
		System.out.println(entities);
	}
}
