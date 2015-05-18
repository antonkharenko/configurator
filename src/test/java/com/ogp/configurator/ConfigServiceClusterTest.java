package com.ogp.configurator;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */

public class ConfigServiceClusterTest {
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static TestingCluster curatorCluster;
	private static  RetryPolicy retryPolicy;	
	private CuratorFramework upsertClient;
	private CuratorFramework getClient;
	private ConfigService upsertConfigService;
	private ConfigService getConfigService;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		curatorCluster = new TestingCluster(3);
		retryPolicy = new RetryNTimes(2, 1000);
		assertNotNull(curatorCluster);
		curatorCluster.start();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		curatorCluster.stop();
		curatorCluster.close();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		assertNotNull(curatorCluster);
		assertNotNull(retryPolicy);
		assertNotNull(curatorCluster.getInstances());
		assertEquals(3, curatorCluster.getInstances().size());
		Iterator<InstanceSpec> it = curatorCluster.getInstances().iterator(); 
		String upsertServerConnString = it.next().getConnectString();
		String getServerConnString = it.next().getConnectString();
		assertNotNull(upsertServerConnString);
		assertNotNull(getServerConnString);
		assertNotEquals(upsertServerConnString, getServerConnString);
		
		System.out.printf("Upsert server %s\n", upsertServerConnString);
		System.out.printf("get server %s\n", getServerConnString);
		
		upsertClient = CuratorFrameworkFactory.newClient(upsertServerConnString, retryPolicy);
		upsertClient.start();
		upsertClient.blockUntilConnected();
		upsertConfigService = ConfigService.newBuilder(upsertClient, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.build();
		upsertConfigService.BlockUntilReady();
		
		getClient = CuratorFrameworkFactory.newClient(getServerConnString, retryPolicy);
		getClient.start();
		getClient.blockUntilConnected();
		getConfigService = ConfigService.newBuilder(upsertClient, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.build();
		getConfigService.BlockUntilReady();
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		upsertClient.close();
		upsertClient = null;
		upsertConfigService = null;
		
		getClient.close();
		getClient = null;
		getConfigService = null;
	}

	@Test
	public void testUpsertConfigEntityOnDifferentNodes() throws Exception {
		//assertNotNull(upsertConfigService);
		assertNotNull(getConfigService);
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			assertTrue(upsertConfigService.upsertConfigEntity(testConfiguration.getId(), testConfiguration));
		} catch (Exception e) {
			fail(e.toString());
		}
		
		long addStart = System.currentTimeMillis();
		
		ServerConfigEntity gotEntity = getConfigService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		while(gotEntity == null) {
			if ((System.currentTimeMillis() - addStart) > 10000) {
				fail("Error: Add entitiy failed, timeout 10sec");
			}
			Thread.sleep(500);
			gotEntity = getConfigService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		}
		assertNotNull(gotEntity);
		assertEquals("10", gotEntity.getId());
		assertEquals("name", gotEntity.getName());
		assertEquals("host", gotEntity.getHost());
		assertEquals(10, gotEntity.getPort());
		
		upsertConfigService.deleteConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		
		long delStart = System.currentTimeMillis();
		
		gotEntity = getConfigService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		while(gotEntity != null) {
			if ((System.currentTimeMillis() - delStart) > 10000) {
				fail("Error: Del entitiy failed, timeout 10sec");
			}
			Thread.sleep(500);
			gotEntity = getConfigService.getConfigEntity(ServerConfigEntity.class, testConfiguration.getId());
		}
	}
}
