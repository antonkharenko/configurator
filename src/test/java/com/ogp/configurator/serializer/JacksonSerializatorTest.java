/**
 * 
 */
package com.ogp.configurator.serializer;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.ogp.configurator.examples.ServerConfigEntity;

/**
 * @author  Andriy Panasenko <avp@avp.kiev.ua>
 *
 */
public class JacksonSerializatorTest {

	private ISerializer serializer;
	
	private byte[] serialize_result = {	0x7B, 0x22, 0x69, 0x64, 0x22, 0x3A, 0x22, 0x31, 
										0x30, 0x22, 0x2C, 0x22, 0x6E, 0x61, 0x6D, 0x65, 
										0x22, 0x3A, 0x22, 0x6E, 0x61, 0x6D, 0x65, 0x22, 
										0x2C, 0x22, 0x68, 0x6F, 0x73, 0x74, 0x22, 0x3A, 
										0x22, 0x68, 0x6F, 0x73, 0x74, 0x22, 0x2C, 0x22, 
										0x70, 0x6F, 0x72, 0x74, 0x22, 0x3A, 0x31, 0x30, 
										0x7D}; 
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		serializer = new JacksonSerializator();
	}

	/**
	 * Test method for {@link com.ogp.configurator.serializer.JacksonSerializator#serialize(java.lang.Object)}.
	 */
	@Test
	public void testSerialize() {
		ServerConfigEntity ent = new ServerConfigEntity("10", "name", "host", 10);
		try {
			byte[] res = serializer.serialize(ent);
			assertEquals(res.length, serialize_result.length);
			assertArrayEquals(serialize_result, res);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	/**
	 * Test method for {@link com.ogp.configurator.serializer.JacksonSerializator#deserialize(byte[], java.lang.Class)}.
	 */
	@Test
	public void testDeserialize() {
		try {
			ServerConfigEntity ent = serializer.deserialize(serialize_result, ServerConfigEntity.class);
			assertEquals("10", ent.getId());
			assertEquals("name", ent.getName());
			assertEquals("host", ent.getHost());
			assertEquals(10, ent.getPort());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
}
