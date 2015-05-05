package com.ogp.configurator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConfigApp {

	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";

	public static void main(String[] args) throws Exception {
		// Start zookeeper client
		final String zookeeperConnectionString = "127.0.0.1:2181";
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();

		// Init config service
		final ConfigService configService = ConfigService.newBuilder(client, ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.build();

		// Upsert (update or insert) config entity
		ServerConfigEntity testConfiguration = new ServerConfigEntity("123", "test-server", "127.0.0.1", 3456);
		configService.upsertConfigEntity(testConfiguration.getId(), testConfiguration);

		// Read config entity
		String configDataAsString = configService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		System.out.println(configDataAsString);

		// List config entities of same type
		List<String> entities = configService.getAllConfigEntities(ServerConfigEntity.class);
		System.out.println(entities);

		// Run some config modifications in separate thread
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				try {
					// Insert new entity
					ServerConfigEntity testConfiguration2 = new ServerConfigEntity("456", "qa-server", "127.0.0.1", 6789);
					configService.upsertConfigEntity(testConfiguration2.getId(), testConfiguration2);

					Thread.sleep(200L);

					// Print local cache
					List<String> entities1 = configService.getAllConfigEntities(ServerConfigEntity.class);
					System.out.println("===" + entities1);

					// Delete entity
					configService.deleteConfigEntity(ServerConfigEntity.class, testConfiguration2.getId());

					Thread.sleep(200L);

					// Print local cache
					List<String> entities2 = configService.getAllConfigEntities(ServerConfigEntity.class);
					System.out.println("===" + entities2);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		//Thread.sleep(5000);
		//exec.shutdown();
	}

}
