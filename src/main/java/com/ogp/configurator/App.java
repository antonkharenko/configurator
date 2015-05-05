package com.ogp.configurator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

	public static void main(String[] args) throws Exception {
		// Start ZK client
		final String zookeeperConnectionString = "127.0.0.1:2181";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();

		// Init config paths
		final String configPath = "/config";
		final String configEnvironmentPath = configPath + "/dev";
		final String configTypePath = configEnvironmentPath + "/TestConfiguration";
		for (String path : Arrays.asList(configPath, configEnvironmentPath, configTypePath)) {
			if (client.checkExists().forPath(path) == null)
				client.create().forPath(path);
		}

		// Create or set config entity
		TestConfiguration testConfiguration = new TestConfiguration("123", "test-server", "127.0.0.1", 3456);
		upsertConfigEntity(client, configTypePath, testConfiguration);

		// Read config entity
		byte[] configData = client.getData().forPath(getConfigEntityPath(configTypePath, testConfiguration));
		String configDataAsString = new String(configData);
		System.out.println(configDataAsString);

		// List config entities of same type
		List<String> entities = client.getChildren().forPath(configTypePath);
		System.out.println(entities);

		// Cache config tree
		final TreeCache configCache = new TreeCache(client, configEnvironmentPath);
		configCache.start();
		configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				System.out.println("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
			}
		});


		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				try {
					// Insert new entity
					TestConfiguration testConfiguration2 = new TestConfiguration("456", "qa-server", "127.0.0.1", 6789);
					upsertConfigEntity(client, configTypePath, testConfiguration2);

					// Check local cache
					Map<String, ChildData> entities = configCache.getCurrentChildren(configTypePath);
					for (ChildData cdata : entities.values()) {
						System.out.println("== " + childDataToString(cdata));
					}


					// Delete entity
					client.delete().forPath(getConfigEntityPath(configTypePath, testConfiguration2));

					entities = configCache.getCurrentChildren(configTypePath);
					for (ChildData cdata : entities.values()) {
						System.out.println("== " + childDataToString(cdata));
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		//Thread.sleep(5000);
		//exec.shutdown();
	}

	private static String childDataToString(ChildData childData) {
		if (childData == null) {
			return "null";
		} else {
			String dataAsString = childData.getData() == null  ? "null" : new String(childData.getData());
			return "path=" + childData.getPath() + " stat=" + childData.getStat() + " data=" + dataAsString;
		}
	}

	private static String getConfigEntityPath(String configTypePath, TestConfiguration configEntity) {
		return configTypePath + "/" + configEntity.getId();
	}

	private static void upsertConfigEntity(CuratorFramework client, String configTypePath, TestConfiguration configEntity) throws Exception {
		final String configEntityPath = getConfigEntityPath(configTypePath, configEntity);
		if (client.checkExists().forPath(configEntityPath) == null) {
			client.create().forPath(configEntityPath, configEntity.toString().getBytes());
		} else {
			client.setData().forPath(configEntityPath, configEntity.toString().getBytes());
		}
	}
}
