package com.ogp.configurator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import java.math.BigDecimal;
import java.util.Hashtable;
import java.util.Map;
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

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */
public class ConfigurationMonitorTest {
	private static final Logger logger = LoggerFactory.getLogger(ConfigurationManagerTest.class);
	
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static final String RATES_TYPE = "FixedCurrencyRates";
	
	private static TestingServer curator;
	private static  RetryPolicy retryPolicy;	
	private CuratorFramework client;
	private ConfigurationManager configManger;
	private ConfigurationMonitor configMonitor;
	private ServerConfigEntity testConfig;
	private FixedCurrencyRates testRate;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		logger.info("ConfigurationMonitorTest() -- setUp() starting....");
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
		
		
		logger.info("ConfigurationMonitorTest() -- setUp() starting.... Done.");
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		logger.info("ConfigurationMonitorTest() -- tearDown() starting....");
		client.close();
		client = null;
		configManger = null;
		configMonitor = null;
		curator.stop();
		curator.close();
		curator = null;
		logger.info("ConfigurationMonitorTest() -- tearDown() starting....Done.");
	}

	@Test
	public void testInitialization() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configManger.save(testConfig.getId(), testConfig);
		
		configManger.save(testRate.getKey(), testRate);
		
		configMonitor = ConfigurationMonitor.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		
		assertNotNull(configMonitor);
		
		final CountDownLatch sync = new CountDownLatch(1);
		
		final Map<String,Object> conf = new Hashtable<String, Object>();
		
		configMonitor.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {}

			@Override
			public void onError(Throwable e) {}

			@Override
			public void onNext(ConfigurationEvent event) {
				if (event.getConfigType() == ConfigType.INITIALIZED) {
					sync.countDown();
				} else if (event.getUpdateType() == UpdateType.ADDED) {
					if (event.getTypeClass() == ServerConfigEntity.class) {
						conf.put("CONF", event.getNewValue());
					} else if (event.getTypeClass() == FixedCurrencyRates.class) {
						conf.put("RATE", event.getNewValue());
					} else {
						fail("Unknown class");
					}
				} else {
					fail("Unknown event");
				}
			}
		});
		
		configMonitor.start();
		
		assertTrue(sync.await(10, TimeUnit.MINUTES));
		
		assertEquals(2, conf.size());
		assertEquals(testConfig, conf.get("CONF"));
		assertEquals(testRate, conf.get("RATE"));
	}
	
	@Test
	public void testAddNewConfig() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configMonitor = ConfigurationMonitor.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		
		assertNotNull(configMonitor);
		
		final CountDownLatch sync = new CountDownLatch(1);
		final CountDownLatch add = new CountDownLatch(1);
		
		final Map<String,Object> conf = new Hashtable<String, Object>();
		
		configMonitor.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {}

			@Override
			public void onError(Throwable e) {}

			@Override
			public void onNext(ConfigurationEvent event) {
				if (event.getConfigType() == ConfigType.INITIALIZED) {
					sync.countDown();
				} else if (event.getUpdateType() == UpdateType.ADDED) {
					if (configMonitor.isInitialized()) {
						conf.put("CONF", event.getNewValue());
						add.countDown();
					}
				} else {
					fail("Unknown event");
				}
			}
		});
		
		configMonitor.start();
		
		assertTrue(sync.await(10, TimeUnit.MINUTES));
		
		configManger.save(testConfig.getId(), testConfig);
		
		assertTrue(add.await(10, TimeUnit.MINUTES));
		
		assertEquals(1, conf.size());
		assertEquals(testConfig, conf.get("CONF"));
	}
	
	@Test
	public void testUpdateConfig() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configMonitor = ConfigurationMonitor.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		
		assertNotNull(configMonitor);
		
		
		
		final CountDownLatch sync = new CountDownLatch(1);
		final CountDownLatch upd = new CountDownLatch(1);
		
		final Map<String,Object> conf = new Hashtable<String, Object>();
		
		configMonitor.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {}

			@Override
			public void onError(Throwable e) {}

			@Override
			public void onNext(ConfigurationEvent event) {
				if (event.getConfigType() == ConfigType.INITIALIZED) {
					sync.countDown();
				} else if (event.getUpdateType() == UpdateType.UPDATED) {
					if (configMonitor.isInitialized()) {
						conf.put("CONF_UPD", event.getNewValue());
						upd.countDown();
					}					
				}
			}
		});
		
		configMonitor.start();
		
		assertTrue(sync.await(1, TimeUnit.MINUTES));
		
		ServerConfigEntity testConfig1 = new ServerConfigEntity("10","name1","host1",11);
		configManger.save(testConfig.getId(), testConfig);
		configManger.save(testConfig1.getId(), testConfig1);
		
		assertTrue(upd.await(1, TimeUnit.MINUTES));
		
		assertEquals(1, conf.size());
		assertEquals(testConfig1, conf.get("CONF_UPD"));
	}
	
	
	@Test
	public void testDeleteConfig() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configManger.save(testConfig.getId(), testConfig);

		configMonitor = ConfigurationMonitor.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		
		assertNotNull(configMonitor);
		
		final CountDownLatch sync = new CountDownLatch(1);
		final CountDownLatch del = new CountDownLatch(1);
		
		configMonitor.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {}

			@Override
			public void onError(Throwable e) {}

			@Override
			public void onNext(ConfigurationEvent event) {
				System.out.println("===" + event);
				if (event.getConfigType() == ConfigType.INITIALIZED) {
					sync.countDown();
				} else if (event.getUpdateType() == UpdateType.REMOVED) {
					if (configMonitor.isInitialized()) {
						del.countDown();
					}					
				}
			}
		});
		
		configMonitor.start();
		
		assertTrue(sync.await(1, TimeUnit.MINUTES));

		configManger.delete(testConfig.getClass(), testConfig.getId());

		assertTrue(del.await(1, TimeUnit.MINUTES));

	}
	
	@Test
	public void testCommunicationMonitor() throws Exception {
		assertNotNull(configManger);
		assertNotNull(testConfig);
		assertNotNull(testRate);
		assertTrue(configManger.isConnected());

		configMonitor = ConfigurationMonitor.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		
		assertNotNull(configMonitor);
		
		
		
		final CountDownLatch sync = new CountDownLatch(1);
		final CountDownLatch conDown = new CountDownLatch(1);
		final CountDownLatch conUp = new CountDownLatch(1);
		
		configMonitor.listen().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {}

			@Override
			public void onError(Throwable e) {}

			@Override
			public void onNext(ConfigurationEvent event) {
				logger.info("onNext ----------------------------------------------------------------- {}",event.getConfigType());
				if (event.getConfigType() == ConfigType.INITIALIZED) {
					logger.info("INTI -----------------------------------------------------------------");
					sync.countDown();
				} else if (event.getConfigType() == ConfigType.CONNECTION_LOST) {
					conDown.countDown();
				} else if (event.getConfigType() == ConfigType.CONNECTION_RESTORED) {
					conUp.countDown();
				}
			}
		});
		
		configMonitor.start();
		
		assertTrue(sync.await(1, TimeUnit.MINUTES));
		
		curator.stop();
		
		assertTrue(conDown.await(1, TimeUnit.MINUTES));
		
		curator.restart();
		
		assertTrue(conUp.await(1, TimeUnit.MINUTES));
	}
}
