/**
 * 
 */
package com.googlecode.jue.test;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.googlecode.jue.util.ConcurrentLRUCache;

/**
 * @author noah
 *
 */
public class ConcurrentLRUCacheTest extends TestCase {
	
	private ConcurrentLRUCache<String, String> cache;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		cache = new ConcurrentLRUCache<String, String>(10);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		cache = null;
	}
	
	public void testPut() {
		for (int i = 0; i < 10; ++i) {
			String key = i + "";
			String value = i + "";
			cache.put(key, value);
		}
		for (int i = 0; i < 10; ++i) {
			String key = i + "";
			String value = i + "";
			Assert.assertEquals(value, cache.get(key));
		}
		Assert.assertEquals(10, cache.size());
	}
}
