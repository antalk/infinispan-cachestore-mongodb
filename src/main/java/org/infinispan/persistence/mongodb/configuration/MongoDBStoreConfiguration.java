package org.infinispan.persistence.mongodb.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.persistence.mongodb.store.MongoDBStore;

/**
 * The configuration of MongoDBStore. <br/>
 * This class wraps all the MongoDB information for the connection.
 *
 * @author Gabriel Francisco &lt;gabfssilva@gmail.com&gt;
 */
@ConfigurationFor(MongoDBStore.class)
@BuiltBy(MongoDBStoreConfigurationBuilder.class)
public class MongoDBStoreConfiguration extends AbstractStoreConfiguration {
   private final String connectionURI;
   private final String collection;

   public MongoDBStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, String connectionURI, String collection) {
      super(attributes, async);
      this.connectionURI = connectionURI;
      this.collection = collection;
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(
              MongoDBStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet());
   }

   public String collection() {
      return collection;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String getConnectionURI() {
      return connectionURI;
   }
   
   @Override
	public boolean segmented() {
		return false;
	}
   

}
