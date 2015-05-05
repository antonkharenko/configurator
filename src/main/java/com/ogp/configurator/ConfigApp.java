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

public class ConfigApp {

	private static final String CONFIG_BASE_PATH = "/config";
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_ENVIRONMENT_PATH = CONFIG_BASE_PATH + "/" + ENVIRONMENT;
	private static final String CONFIG_GROUP = "servers";
	private static final String CONFIG_GROUP_PATH = CONFIG_ENVIRONMENT_PATH + "/" + CONFIG_GROUP;


	public static void main(String[] args) throws Exception {
		// Start zookeeper client
		final String zookeeperConnectionString = "127.0.0.1:2181";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();

		// Init config paths
		for (String path : Arrays.asList(CONFIG_BASE_PATH, CONFIG_ENVIRONMENT_PATH, CONFIG_GROUP_PATH)) {
			if (client.checkExists().forPath(path) == null)
				client.create().forPath(path);
		}

		// Upsert (update or insert) config entity
		ServerConfigEntity testConfiguration = new ServerConfigEntity("123", "test-server", "127.0.0.1", 3456);
		upsertConfigEntity(client, CONFIG_GROUP_PATH, testConfiguration);

		// Read config entity
		byte[] configData = client.getData().forPath(getConfigEntityPath(CONFIG_GROUP_PATH, testConfiguration));
		String configDataAsString = new String(configData);
		System.out.println(configDataAsString);

		// List config entities of same type
		List<String> entities = client.getChildren().forPath(CONFIG_GROUP_PATH);
		System.out.println(entities);

		// Init config cache
		final TreeCache configCache = new TreeCache(client, CONFIG_ENVIRONMENT_PATH);
		configCache.start();
		configCache.getListenable().addListener(new TreeCacheListener() {
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				System.out.println("== Config event: type=" + event.getType() + " childData={" + childDataToString(event.getData()) + "}");
			}
		});

		// Run some config modifications in separate thread
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				try {
					// Insert new entity
					ServerConfigEntity testConfiguration2 = new ServerConfigEntity("456", "qa-server", "127.0.0.1", 6789);
					upsertConfigEntity(client, CONFIG_GROUP_PATH, testConfiguration2);

					// Print local cache
					Map<String, ChildData> entities = configCache.getCurrentChildren(CONFIG_GROUP_PATH);
					for (ChildData cdata : entities.values()) {
						System.out.println("== " + childDataToString(cdata));
					}

					// Delete entity
					client.delete().forPath(getConfigEntityPath(CONFIG_GROUP_PATH, testConfiguration2));

					// Print local cache
					entities = configCache.getCurrentChildren(CONFIG_GROUP_PATH);
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

	private static String getConfigEntityPath(String configTypePath, ServerConfigEntity configEntity) {
		return configTypePath + "/" + configEntity.getId();
	}

	private static void upsertConfigEntity(CuratorFramework client, String configTypePath, ServerConfigEntity configEntity) throws Exception {
		final String configEntityPath = getConfigEntityPath(configTypePath, configEntity);
		if (client.checkExists().forPath(configEntityPath) == null) {
			client.create().forPath(configEntityPath, configEntity.toString().getBytes());
		} else {
			client.setData().forPath(configEntityPath, configEntity.toString().getBytes());
		}
	}
}
