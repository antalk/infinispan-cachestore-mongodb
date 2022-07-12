package org.infinispan.persistence.mongodb.cache;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.testng.annotations.Test;

public class CacheImplTest {
	
	@Test(testName = "testCache")
	public void testCache() throws IOException {
		
		DefaultCacheManager cacheManager = new DefaultCacheManager(this.getClass().getResourceAsStream("/config/mongodb-config.xml"));
	     Cache<Object,Object> cache = cacheManager.getCache("cache");
	     
	     cache.put("a", "b");
	     
	     cache.keySet().forEach(o -> System.err.println(o));
	     
	     
	     cache.clear();
	}

}
