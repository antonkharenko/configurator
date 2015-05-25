package com.ogp.configurator;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
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
import rx.functions.Func1;

import com.ogp.configurator.ConfigurationEvent.ConfigType;
import com.ogp.configurator.ConfigurationEvent.UpdateType;
import com.ogp.configurator.examples.ServerConfigEntity;
import com.ogp.configurator.serializer.JacksonSerializator;

/**
 * @author Andriy Panasenko <avp@avp.kiev.ua>
 *
 */

public class ConfigServiceInitializationTest {
	private static final Logger logger = LoggerFactory.getLogger(ConfigServiceInitializationTest.class);
	
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
		final CountDownLatch waitInitObj = new CountDownLatch(1);
		saveConfigService.listenUpdates()
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
		saveConfigService.start();
		
		assertTrue(waitInitObj.await(5, TimeUnit.MINUTES));
	
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		saveClient.close();
		saveClient = null;
		saveConfigService = null;
		
		if (getClient != null) {
			getClient.close();
			getClient = null;
			getConfigService = null;
		}
	}

	@Test
	public void testInitializationService() throws Exception {
		assertNotNull(saveConfigService);
		
		final int NUMBER_OF_ENTITIES = 1000;
		
		try {
			for(int i=0;i<NUMBER_OF_ENTITIES;i++) {
				saveConfigService.save(String.valueOf(i), getServerConfigEntity(i));
			}
		} catch (Exception e) {
			fail(e.toString());
		}
		Iterator<InstanceSpec> it = curatorCluster.getInstances().iterator(); 
		String saveServerConnString = it.next().getConnectString();
		String getServerConnString = it.next().getConnectString();
		assertNotNull(saveServerConnString);
		assertNotNull(getServerConnString);
		assertNotEquals(saveServerConnString, getServerConnString);
		getClient = CuratorFrameworkFactory.newClient(getServerConnString, retryPolicy);
		getClient.start();
		getClient.blockUntilConnected();
		getConfigService = ConfigService.newBuilder(getClient, new JacksonSerializator(), ENVIRONMENT)
				.registerConfigType(CONFIG_TYPE, ServerConfigEntity.class)
				.build();
		final List<ConfigurationEvent> receivedEvents = new ArrayList<ConfigurationEvent>();
		
		final CountDownLatch waitInitObj = new CountDownLatch(1);
		getConfigService.listenUpdates().subscribe(new Observer<ConfigurationEvent>() {

			@Override
			public void onCompleted() {
				
			}

			@Override
			public void onError(Throwable e) {
				
			}

			@Override
			public void onNext(ConfigurationEvent t) {
				if (t.getConfigType() == ConfigType.INITIALIZED)
					waitInitObj.countDown();
				else if(t.getConfigType() == ConfigType.UNDEFINED) {
					if (t.getUpdateType() == UpdateType.ADDED) {
						receivedEvents.add(t);
					}
				}
			}
		});
		getConfigService.start();
		
		assertTrue(waitInitObj.await(5, TimeUnit.MINUTES));
		
		assertEquals(NUMBER_OF_ENTITIES, receivedEvents.size());
		
		logger.info("Test Initialization complete, got {} updates", receivedEvents.size());
	}
	
	private ServerConfigEntity getServerConfigEntity(int i) {
		return new ServerConfigEntity(String.valueOf(i),"name"+String.valueOf(i),"host"+String.valueOf(i),i*10);
	}
}
