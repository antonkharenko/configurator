/**
 * 
 */
package com.ogp.configurator;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
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
import rx.functions.Func1;

import com.ogp.configurator.ConfigurationEvent.ConfigType;
import com.ogp.configurator.ConfigurationEvent.UpdateType;
import com.ogp.configurator.examples.FixedCurrencyRates;
import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */
public class ConfigServiceTest {
	
	private static final Logger logger = LoggerFactory.getLogger(ConfigServiceTest.class);
	
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static final String RATES_TYPE = "FixedCurrencyRates";
	
	private static TestingServer curator;
	private static  RetryPolicy retryPolicy;	
	private CuratorFramework client;
	private ConfigService configService;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		logger.info("ConfigServiceTest() -- setUp() starting....");
		curator = new TestingServer(true);
		retryPolicy = new ExponentialBackoffRetry(1000, 3);
		
		assertNotNull(curator);
		assertNotNull(retryPolicy);
		client = CuratorFrameworkFactory.newClient(curator.getConnectString(), retryPolicy);
		assertNotNull(client);
		client.start();
		client.blockUntilConnected();
		configService = ConfigService.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();

		assertNotNull(configService);

		final CountDownLatch waitInitObj = new CountDownLatch(1);
		configService.listenUpdates()
			.filter(new Func1<ConfigurationEvent, Boolean>() {
				@Override
				public Boolean call(ConfigurationEvent t1) {
					return(t1.getConfigType() == ConfigType.INITIALIZED);
				}
			})
			.subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {
				
			}

			@Override
			public void onError(Throwable e) {
				
			}

			@Override
			public void onNext(ConfigurationEvent t) {
					waitInitObj.countDown();
			}
		});
		configService.start();
		
		assertTrue(waitInitObj.await(5, TimeUnit.MINUTES));
		
		logger.info("ConfigServiceTest() -- setUp() starting.... Done.");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		logger.info("ConfigServiceTest() -- tearDown() starting....");
		client.close();
		client = null;
		configService = null;
		curator.stop();
		curator.close();
		curator = null;
		logger.info("ConfigServiceTest() -- tearDown() starting....Done.");
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#save(java.lang.String, java.lang.Object)}.
	 */
	@Test
	public void testSaveStringT() {
		logger.info("ConfigServiceTest() -- testSaveStringT() starting....");
		assertNotNull(configService);
		final CountDownLatch waitSaveObj = new CountDownLatch(1);
		final List<ConfigurationEvent> receivedEvents = new ArrayList<ConfigurationEvent>();
		
		configService.listenUpdates().subscribe(new Observer<ConfigurationEvent>() {

				@Override
				public void onCompleted() {
					
				}
	
				@Override
				public void onError(Throwable e) {
					
				}
	
				@Override
				public void onNext(ConfigurationEvent t) {
					logger.debug("{}: type={}",t.toString(),t.getUpdateType());
					receivedEvents.add(t);
					waitSaveObj.countDown();
				}
		});
		
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			configService.save(testConfiguration.getId(), testConfiguration);
		} catch (Exception e) {
			fail(e.toString());
		}
		
		try {
			assertTrue(waitSaveObj.await(1, TimeUnit.MINUTES));
			assertEquals(1, receivedEvents.size());
			assertTrue(UpdateType.ADDED == receivedEvents.get(0).getUpdateType());
			assertEquals(testConfiguration, receivedEvents.get(0).getNewValue());
		} catch (InterruptedException e1) {
			fail(e1.toString());
		}
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#get(java.lang.Class, java.lang.String)}.
	 */
	@Test
	public void testGet() {
		logger.info("ConfigServiceTest() -- testGet() starting....");
		assertNotNull(configService);
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			configService.save(testConfiguration.getId(), testConfiguration);
			ServerConfigEntity gotEntity = configService.get(ServerConfigEntity.class, testConfiguration.getId());
			assertNotNull(gotEntity);
			assertEquals(testConfiguration, gotEntity);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#list(java.lang.Class)}.
	 */
	@Test
	public void testList() {
		logger.info("ConfigServiceTest() -- testList() starting....");
		assertNotNull(configService);
		ServerConfigEntity testConfiguration1 = new ServerConfigEntity("11","name1","host1",11);
		ServerConfigEntity testConfiguration2 = new ServerConfigEntity("12","name2","host2",12);
		FixedCurrencyRates rates1 = new FixedCurrencyRates("RATES1");
		rates1
			.addRate("USD", new BigDecimal(1.01))
			.addRate("UAH", new BigDecimal(21.11))
			.addRate("EUR", new BigDecimal(1.31));
		FixedCurrencyRates rates2 = new FixedCurrencyRates("RATES2");
		rates2
			.addRate("USD", new BigDecimal(2.01))
			.addRate("UAH", new BigDecimal(22.11))
			.addRate("EUR", new BigDecimal(2.31));
		try {
			configService.save(testConfiguration1.getId(), testConfiguration1);
			configService.save(testConfiguration2.getId(), testConfiguration2);
			configService.save(rates1.getKey(), rates1);
			configService.save(rates2.getKey(), rates2);
			
			List<ServerConfigEntity> confEnts = configService.list(ServerConfigEntity.class);
			assertNotNull(confEnts);
			
			for (ServerConfigEntity serverConfigEntity : confEnts) {
				logger.debug("Entity: {}", serverConfigEntity.toString());
			}
			assertEquals(2, confEnts.size());
			for (Object ent : confEnts) {
				ServerConfigEntity e = (ServerConfigEntity)ent;
				if ("11".equals(e.getId())) {
					assertEquals("name1", e.getName());
					assertEquals("host1", e.getHost());
					assertEquals(11, e.getPort());
				} else if ("12".equals(e.getId())) {
					assertEquals("name2", e.getName());
					assertEquals("host2", e.getHost());
					assertEquals(12, e.getPort());
				} else {
					fail("Error getAllConfigEntities return unknow entity "+e.getId());
				}
			}
			
			List<FixedCurrencyRates> rates = configService.list(FixedCurrencyRates.class);
			assertNotNull(confEnts);
			assertEquals(2, confEnts.size());
			for (Object rate : rates) {
				FixedCurrencyRates r = (FixedCurrencyRates)rate;
				if ("RATES1".equals(r.getKey())) {
					Map<String, BigDecimal> r1 = r.getRates();
					assertNotNull(r1);
					assertEquals(3, r1.size());
					assertNotNull(r1.get("USD"));
					assertNotNull(r1.get("UAH"));
					assertNotNull(r1.get("EUR"));
					assertEquals(new BigDecimal(1.01), r1.get("USD"));
					assertEquals(new BigDecimal(21.11), r1.get("UAH"));
					assertEquals(new BigDecimal(1.31), r1.get("EUR"));
				} else if ("RATES2".equals(r.getKey())) {
					Map<String, BigDecimal> r2 = r.getRates();
					assertNotNull(r2);
					assertEquals(3, r2.size());
					assertNotNull(r2.get("USD"));
					assertNotNull(r2.get("UAH"));
					assertNotNull(r2.get("EUR"));
					assertEquals(new BigDecimal(2.01), r2.get("USD"));
					assertEquals(new BigDecimal(22.11), r2.get("UAH"));
					assertEquals(new BigDecimal(2.31), r2.get("EUR"));
				} else {
					fail("Error getAllConfigEntities return unknow rate "+r.getKey());
				}
			}
			
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#delete(java.lang.Class, java.lang.String)}.
	 */
	@Test
	public void testDelete() {
		logger.info("ConfigServiceTest() -- testDelete() starting....");
		assertNotNull(configService);
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			configService.save(testConfiguration.getId(), testConfiguration);
		} catch (Exception e) {
			fail(e.toString());
		}
		try {
			configService.delete(ServerConfigEntity.class, "10");
		} catch (ConnectionLossException e) {
			fail(e.toString());
		}
		ServerConfigEntity delEntity = configService.get(ServerConfigEntity.class, testConfiguration.getId());
		assertNull(delEntity);
	}

}
