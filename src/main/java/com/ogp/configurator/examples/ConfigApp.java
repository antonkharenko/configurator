package com.ogp.configurator.examples;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.ogp.configurator.ConfigService;
import com.ogp.configurator.serializer.JacksonSerializator;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConfigApp {

	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static final String RATES_TYPE = "FixedCurrencyRates";

	public static void main(String[] args) throws Exception {
		// Start zookeeper client
		String zookeeperConnectionString = "127.0.0.1:2181";
		if (args.length < 1) {
			System.out.println("Inter connection string.");
			System.out.println("Usage: app <host:port>");
			System.out.println("Default 127.0.0.1:2181 will be used.");
		} else {
			zookeeperConnectionString = args[0];
			System.out.println("Connection "+zookeeperConnectionString+" used.");
		}
		final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
		client.start();

		// Init config service
		final ConfigService configService = ConfigService.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();

		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				while(true) {
					try {
						// Insert new entity
	
						Thread.sleep(60000L);
						
						List<Object> allEntities = configService.getAllConfigEntities(ServerConfigEntity.class);
						System.out.printf("Total %d objects:\n", allEntities.size());
						for (Object object : allEntities) {
							System.out.printf("Object %s\n", object.toString());
						}
						System.out.println("----------------------------------------------------------");
						System.out.println("");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		//Thread.sleep(5000);
		//exec.shutdown();
	}

}
