/**
 * 
 */
package com.ogp.configurator;

import static org.junit.Assert.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ogp.configurator.examples.FixedCurrencyRates;
import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */
public class ConfigServiceTest {
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
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		curator = new TestingServer(true);
		retryPolicy = new ExponentialBackoffRetry(1000, 3);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		curator.stop();
		curator.close();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		assertNotNull(curator);
		assertNotNull(retryPolicy);
		client = CuratorFrameworkFactory.newClient(curator.getConnectString(), retryPolicy);
		client.start();
		client.blockUntilConnected();
		configService = ConfigService.newBuilder(client, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.registerConfigType(RATES_TYPE, FixedCurrencyRates.class)
				.build();
		configService.awaitConnected();		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		client.close();
		client = null;
		configService = null;
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#upsertConfigEntity(java.lang.String, java.lang.Object)}.
	 */
	@Test
	public void testUpsertConfigEntityStringT() {
		assertNotNull(configService);
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			assertTrue(configService.upsertConfigEntity(testConfiguration.getId(), testConfiguration));
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#getConfigEntity(java.lang.Class, java.lang.String)}.
	 */
	@Test
	public void testGetConfigEntity() {
		assertNotNull(configService);
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			assertTrue(configService.upsertConfigEntity(testConfiguration.getId(), testConfiguration));
			ServerConfigEntity gotEntity = configService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
			assertNotNull(gotEntity);
			assertEquals("10", gotEntity.getId());
			assertEquals("name", gotEntity.getName());
			assertEquals("host", gotEntity.getHost());
			assertEquals(10, gotEntity.getPort());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	/**
	 * Test method for {@link com.ogp.configurator.ConfigService#getAllConfigEntities(java.lang.Class)}.
	 */
	@Test
	public void testGetAllConfigEntities() {
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
			assertTrue(configService.upsertConfigEntity(testConfiguration1.getId(), testConfiguration1));
			assertTrue(configService.upsertConfigEntity(testConfiguration2.getId(), testConfiguration2));
			assertTrue(configService.upsertConfigEntity(rates1.getKey(), rates1));
			assertTrue(configService.upsertConfigEntity(rates2.getKey(), rates2));
			
			List<Object> confEnts = configService.getAllConfigEntities(ServerConfigEntity.class);
			assertNotNull(confEnts);
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
			
			List<Object> rates = configService.getAllConfigEntities(FixedCurrencyRates.class);
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
	 * Test method for {@link com.ogp.configurator.ConfigService#deleteConfigEntity(java.lang.Class, java.lang.String)}.
	 */
	@Test
	public void testDeleteConfigEntity() {
		assertNotNull(configService);
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			assertTrue(configService.upsertConfigEntity(testConfiguration.getId(), testConfiguration));
		} catch (Exception e) {
			fail(e.toString());
		}
		configService.deleteConfigEntity(ServerConfigEntity.class, "10");
		ServerConfigEntity delEntity = configService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		assertNull(delEntity);
	}

}
