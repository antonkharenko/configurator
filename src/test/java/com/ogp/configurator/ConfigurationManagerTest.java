package com.ogp.configurator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ogp.configurator.examples.FixedCurrencyRates;
import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */

public class ConfigurationManagerTest {
	private static final Logger logger = LoggerFactory.getLogger(ConfigurationManagerTest.class);
	
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static final String RATES_TYPE = "FixedCurrencyRates";
	
	private static TestingServer curator;
	private static  RetryPolicy retryPolicy;	
	private CuratorFramework client;
	private ConfigurationManager configManger;
	
	private ServerConfigEntity testConfig;
	private FixedCurrencyRates testRate;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		logger.info("ConfigurationManagerTest() -- setUp() starting....");
		curator = new TestingServer(true);
		retryPolicy = new ExponentialBackoffRetry(1000, 3);
		
		assertNotNull(curator);
		assertNotNull(retryPolicy);
		client = CuratorFrameworkFactory.newClient(curator.getConnectString(), retryPolicy);
		assertNotNull(client);
		client.start();
		client.blockUntilConnected();
		configManger = ConfigurationManager.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();

		assertNotNull(configManger);
		configManger.start();
		
		testConfig = new ServerConfigEntity("10","name","host",10);
		testRate = new FixedCurrencyRates("RATES");
		testRate
			.addRate("USD", new BigDecimal(1.01))
			.addRate("UAH", new BigDecimal(21.11))
			.addRate("EUR", new BigDecimal(1.31));
		
		
		logger.info("ConfigurationManagerTest() -- setUp() starting.... Done.");
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		logger.info("ConfigurationManagerTest() -- tearDown() starting....");
		client.close();
		client = null;
		configManger = null;
		curator.stop();
		curator.close();
		curator = null;
		logger.info("ConfigurationManagerTest() -- tearDown() starting....Done.");
	}
	
	/**
	 * Test method for {@link com.ogp.configurator.ConfigurationManager#save(String, Object)}
	 * @throws Exception
	 */
	@Test
	public void testSave() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configManger.save(testConfig.getId(), testConfig);
		
		configManger.save(testRate.getKey(), testRate);
	}
	
	/**
	 * Test method for {@link com.ogp.configurator.ConfigurationManager#get(Class, String)}}
	 * @throws Exception
	 */
	@Test
	public void testGet() throws Exception {
		testSave();

		ServerConfigEntity s = configManger.get(ServerConfigEntity.class, testConfig.getId());
		assertNotNull(s);
		assertEquals(testConfig, s);
		FixedCurrencyRates r = configManger.get(FixedCurrencyRates.class, testRate.getKey());
		assertNotNull(r);
		assertEquals(testRate, r);
	}
	
	/**
	 * Test method for {@link com.ogp.configurator.ConfigurationManager#list(Class)}}
	 * @throws Exception
	 */
	@Test
	public void testList() throws Exception {
		testSave();
		
		List<ServerConfigEntity> sl = configManger.list(ServerConfigEntity.class);
		assertNotNull(sl);
		assertEquals(1, sl.size());
		assertEquals(testConfig, sl.get(0));
		
		List<FixedCurrencyRates> rl = configManger.list(FixedCurrencyRates.class);
		assertNotNull(rl);
		assertEquals(1, rl.size());
		assertEquals(testRate, rl.get(0));
	}
	
	/**
	 * Test method for {@link com.ogp.configurator.ConfigurationManager#delete(Class, String)}}
	 * @throws Exception
	 */
	@Test
	public void testDelete() throws Exception {
		testGet();
		
		configManger.delete(ServerConfigEntity.class, testConfig.getId());
		configManger.delete(FixedCurrencyRates.class, testRate.getKey());
		
		ServerConfigEntity s = configManger.get(ServerConfigEntity.class, testConfig.getId());
		assertNull(s);
		FixedCurrencyRates r = configManger.get(FixedCurrencyRates.class, testRate.getKey());
		assertNull(r);
	}
	
	@Test
	public void testIsConnected() throws Exception {
		assertTrue(configManger.isConnected());
		curator.stop();
		Thread.sleep(1000);
		assertFalse(configManger.isConnected());
		logger.info("Starting curator");
		curator.restart();
		configManger.awaitConnected();
		assertTrue(configManger.isConnected());
	}
}
