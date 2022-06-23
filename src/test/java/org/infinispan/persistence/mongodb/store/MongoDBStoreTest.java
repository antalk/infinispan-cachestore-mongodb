package org.infinispan.persistence.mongodb.store;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.mongodb.configuration.MongoDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for MongoDBStoreTest
 * In order to run the test suite, you need to start a MongoDB server instance and set
 * the MONGODB_HOSTNAME and MONGODB_PORT variables. <br/>
 * For example, if your MongoDB is running local, you should set MONGODB_HOSTNAME as localhost and MONGODB_PORT as 27017
 * (or your mongodb port)
 *
 * @author Gabriel Francisco <gabfssilva@gmail.com>
 */
@Test(groups = "unit", testName = "org.infinispan.persistence.mongodb.store.MongoDBStoreTest")
public class MongoDBStoreTest extends BaseNonBlockingStoreTest {

   public static final String DATABASE = "mongostoretest";
   public static final String COLLECTION = "mongostoretest";

   private MongoDBStore mongoDBStore;

   @Override
   protected NonBlockingStore createStore() throws Exception {
         mongoDBStore = new MongoDBStore();
   
      return mongoDBStore;
   }
   
   @AfterMethod
   public void tearDown() {
      mongoDBStore.clear();
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
	   
	   String hostname = System.getProperty("MONGODB_HOSTNAME");
	      if (hostname == null || "".equals(hostname)) {
	         hostname = "127.0.0.1";
	      }

	      int port = 27017;
	      String configurationPort = System.getProperty("MONGODB_PORT");
	      try {
	         if (configurationPort != null && !"".equals(configurationPort)) {
	            port = Integer.parseInt(configurationPort);
	         }
	      } catch (NumberFormatException e) {
	         throw e;
	      }
	   
	   
	   
	   return configurationBuilder
	       .persistence()
	       .addStore(MongoDBStoreConfigurationBuilder.class)
	       .connectionURI("mongodb://test:test@" + hostname + ":" + port + "/" + DATABASE + "?connectTimeoutMS=1000&w=1")
	       .collection(COLLECTION)
	       .build();

   }
   
}
