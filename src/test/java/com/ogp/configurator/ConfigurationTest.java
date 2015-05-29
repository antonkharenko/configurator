package com.ogp.configurator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

import rx.Observer;

import com.ogp.configurator.ConfigurationEvent.ConfigType;
import com.ogp.configurator.ConfigurationEvent.UpdateType;
import com.ogp.configurator.examples.FixedCurrencyRates;
import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

public class ConfigurationTest {
private static final Logger logger = LoggerFactory.getLogger(ConfigurationTest.class);
	
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static final String RATES_TYPE = "FixedCurrencyRates";
	
	private static TestingServer curator;
	private static  RetryPolicy retryPolicy;	
	private CuratorFramework client;
	private ConfigurationManager configManger;
	private Configuration config;
	private ServerConfigEntity testConfig;
	private FixedCurrencyRates testRate;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		logger.info("ConfigurationTest() -- setUp() starting....");
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
		
		
		logger.info("ConfigurationTest() -- setUp() starting.... Done.");
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		logger.info("ConfigurationTest() -- tearDown() starting....");
		client.close();
		client = null;
		configManger = null;
		config = null;
		curator.stop();
		curator.close();
		curator = null;
		logger.info("ConfigurationTest() -- tearDown() starting....Done.");
	}

	@Test
	public void testInitialization() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configManger.save(testConfig.getId(), testConfig);
		
		configManger.save(testRate.getKey(), testRate);
		
		config = Configuration.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		
		final CountDownLatch sync = new CountDownLatch(1);
		
		config.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {}

			@Override
			public void onError(Throwable e) {e.printStackTrace();}

			@Override
			public void onNext(ConfigurationEvent event) {
				logger.trace("testInitialization() onNext() event: {}", event.toString()); 
				if (event.getConfigType() == ConfigType.INITIALIZED)
					sync.countDown();
			}
		});
		
		config.start();
		assertNotNull(config);
		assertTrue(config.awaitInitialized(10, TimeUnit.MINUTES));
		assertTrue(sync.await(1, TimeUnit.MINUTES));
	}
	
	@Test
	public void testGet() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());
		
		config = Configuration.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		assertNotNull(config);
		
		configManger.save(testConfig.getId(), testConfig);
		
		configManger.save(testRate.getKey(), testRate);
		
		config.start();
		
		assertTrue(config.awaitInitialized(1, TimeUnit.MINUTES));
		
		ServerConfigEntity s = config.get(ServerConfigEntity.class, testConfig.getId());
		assertEquals(testConfig, s);
		
		FixedCurrencyRates r = config.get(FixedCurrencyRates.class, testRate.getKey());
		assertEquals(testRate, r);
	}
	
	@Test
	public void testList() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());
		
		config = Configuration.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		assertNotNull(config);
		
		configManger.save(testConfig.getId(), testConfig);
		
		configManger.save(testRate.getKey(), testRate);
		
		config.start();
		
		assertTrue(config.awaitInitialized(1, TimeUnit.MINUTES));
		
		List<ServerConfigEntity> sl = config.list(ServerConfigEntity.class);
		assertEquals(1, sl.size());
		assertEquals(testConfig, sl.get(0));
		
		List<FixedCurrencyRates> rl = config.list(FixedCurrencyRates.class);
		assertEquals(1, rl.size());
		assertEquals(testRate, rl.get(0));
	}
	
	@Test
	public void testUpdateObservable() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());
		
		config = Configuration.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		assertNotNull(config);
		
		final List<ConfigurationEvent> events = new ArrayList<ConfigurationEvent>();
		final CountDownLatch upd = new CountDownLatch(1);
		
		config.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {
				logger.error("Got complete from Configuration");
				fail("Got complete from Configuration");
			}

			@Override
			public void onError(Throwable e) {
				logger.error("Got onError from Configuration",e);
				fail(e.toString());
			}

			@Override
			public void onNext(ConfigurationEvent event) {
				events.add(event);
				if (event.getUpdateType() == UpdateType.UPDATED
						&& event.getTypeClass() == ServerConfigEntity.class)
					upd.countDown();
			}
		});
		config.start();
		
		configManger.save(testConfig.getId(), testConfig);
		ServerConfigEntity newConfig = new ServerConfigEntity(testConfig.getId(), "name1", "host1",11);
		configManger.save(newConfig.getId(), newConfig);
		
		assertTrue(upd.await(1, TimeUnit.MINUTES));
		
		assertEquals(3, events.size());
		assertEquals(ConfigType.INITIALIZED, events.get(0).getConfigType());
		assertEquals(UpdateType.ADDED, events.get(1).getUpdateType());
		assertEquals(testConfig, events.get(1).getNewValue());
		assertEquals(UpdateType.UPDATED, events.get(2).getUpdateType());
		assertEquals(newConfig, events.get(2).getNewValue());
		assertEquals(testConfig, events.get(2).getOldValue());
		
	}
	
	@Test
	public void testReconnectObservable() throws Exception {
		config = Configuration.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		assertNotNull(config);
		
		final CountDownLatch stopWait = new CountDownLatch(1);
		final CountDownLatch restartWait = new CountDownLatch(1);
		
		config.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {
				logger.error("Got complete from Configuration");
				fail("Got complete from Configuration");
			}

			@Override
			public void onError(Throwable e) {
				logger.error("Got onError from Configuration",e);
				fail(e.toString());
				
			}

			@Override
			public void onNext(ConfigurationEvent event) {
				if (event.getConfigType() == ConfigType.CONNECTION_LOST) {
					stopWait.countDown();
				} else if (event.getConfigType() == ConfigType.CONNECTION_RESTORED) {
					restartWait.countDown();
				}
			}
		});
		
		config.start();
		assertTrue(config.awaitInitialized(1, TimeUnit.MINUTES));
		curator.stop();
		assertTrue(stopWait.await(1, TimeUnit.MINUTES));
		
		curator.restart();
		assertTrue(restartWait.await(1, TimeUnit.MINUTES));
	}
}
