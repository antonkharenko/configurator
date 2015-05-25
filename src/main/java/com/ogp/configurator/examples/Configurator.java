package com.ogp.configurator.examples;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.ogp.configurator.ConfigService;
import com.ogp.configurator.ConnectionLossException;
import com.ogp.configurator.serializer.JacksonSerializator;


public class Configurator {
	
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static final String RATES_TYPE = "FixedCurrencyRates";
	
	private static Random rnd = new Random(System.currentTimeMillis());
	
	public static void main(String[] args) {
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

		try {
			configService.start();
		} catch (ConnectionLossException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		// Run some config modifications in separate thread
		
		FixedCurrencyRates rates = new FixedCurrencyRates("RATES");
		rates
			.addRate("USD", new BigDecimal(1.01))
			.addRate("UAH", new BigDecimal(21.11))
			.addRate("EUR", new BigDecimal(1.31));
		try {
			if (!configService.isConnected())
				configService.awaitConnected();
			
			configService.save(rates.getKey(), rates);

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.submit(new Runnable() {
			public void run() {
				while(true) {
					try {
						// Insert new entity
						if (!configService.isConnected())
							configService.awaitConnected();

						ServerConfigEntity testConfiguration = new ServerConfigEntity(
								String.valueOf(rnd.nextInt(1000000)), 
								getRandomString(10), 
								getRandomString(5), 
								rnd.nextInt(10000));
						configService.save(testConfiguration.getId(), testConfiguration);
						
						
	
						Thread.sleep(60000L);
	
						
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		
	}

	public static String getRandomString(int length) {
       final String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJLMNOPQRSTUVWXYZ1234567890_";
       StringBuilder result = new StringBuilder();
       while(length > 0) {
           Random rand = new Random();
           result.append(characters.charAt(rand.nextInt(characters.length())));
           length--;
       }
       return result.toString();
    }
	

}
