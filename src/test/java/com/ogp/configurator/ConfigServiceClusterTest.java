package com.ogp.configurator;

import static org.junit.Assert.*;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observer;

import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */

public class ConfigServiceClusterTest {
	private static final Logger logger = LoggerFactory.getLogger(ConfigServiceClusterTest.class);
	
	private static final String ENVIRONMENT = "local";
	private static final String CONFIG_TYPE = "server";
	private static TestingCluster curatorCluster;
	private static  RetryPolicy retryPolicy;	
	private CuratorFramework saveClient;
	private CuratorFramework getClient;
	private ConfigService saveConfigService;
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
		String saveServerConnString = it.next().getConnectString();
		String getServerConnString = it.next().getConnectString();
		assertNotNull(saveServerConnString);
		assertNotNull(getServerConnString);
		assertNotEquals(saveServerConnString, getServerConnString);
		
		logger.info("Save server connect string %s\n", saveServerConnString);
		logger.info("Get server %s\n", getServerConnString);
		
		saveClient = CuratorFrameworkFactory.newClient(saveServerConnString, retryPolicy);
		saveClient.start();
		saveClient.blockUntilConnected();
		saveConfigService = ConfigService.newBuilder(saveClient, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.build();
		saveConfigService.start();
		
		getClient = CuratorFrameworkFactory.newClient(getServerConnString, retryPolicy);
		getClient.start();
		getClient.blockUntilConnected();
		getConfigService = ConfigService.newBuilder(getClient, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.build();
		getConfigService.start();
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		saveClient.close();
		saveClient = null;
		saveConfigService = null;
		
		getClient.close();
		getClient = null;
		getConfigService = null;
	}

	@Test
	public void testSaveConfigEntityOnDifferentNodes() throws Exception {
		assertNotNull(saveConfigService);
		assertNotNull(getConfigService);
		
		final Hashtable<String, ConfigurationUpdateEvent> receivedConfigUpdates = new Hashtable<String, ConfigurationUpdateEvent>();
		final CountDownLatch waitSaveEvents = new CountDownLatch(2); //Wait two events on Save node and Get node
		saveConfigService.listenUpdates().subscribe(new Observer<ConfigurationUpdateEvent>() {

			@Override
			public void onCompleted() {
				
			}

			@Override
			public void onError(Throwable e) {
				
			}

			@Override
			public void onNext(ConfigurationUpdateEvent t) {
				logger.debug("saveConfigService() {}: type={}",t.toString(),t.getUpdateType());
				receivedConfigUpdates.put("SAVE", t);
				waitSaveEvents.countDown();
			}
		});
		
		getConfigService.listenUpdates().subscribe(new Observer<ConfigurationUpdateEvent>() {

			@Override
			public void onCompleted() {
				
			}

			@Override
			public void onError(Throwable e) {
				
			}

			@Override
			public void onNext(ConfigurationUpdateEvent t) {
				logger.debug("getConfigService() {}: type={}",t.toString(),t.getUpdateType());
				receivedConfigUpdates.put("GET", t);
				waitSaveEvents.countDown();
			}
		});
		
		//Save new ConfigEntity
		ServerConfigEntity testConfiguration = new ServerConfigEntity("10","name","host",10);
		try {
			saveConfigService.save(testConfiguration.getId(), testConfiguration);
		} catch (Exception e) {
			fail(e.toString());
		}
		//Wait until both services got events
		assertTrue(waitSaveEvents.await(1, TimeUnit.MINUTES));
		
		//Check that both entity in updates equals
		assertEquals(2, receivedConfigUpdates.size());
		assertTrue(receivedConfigUpdates.containsKey("GET"));
		assertTrue(receivedConfigUpdates.containsKey("SAVE"));
		assertEquals(receivedConfigUpdates.get("GET").getNewValue(), receivedConfigUpdates.get("SAVE").getNewValue());
		
	}
}
