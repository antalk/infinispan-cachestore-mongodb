package org.infinispan.persistence.mongodb.configuration.parser;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.persistence.mongodb.configuration.MongoDBStoreConfigurationBuilder;

/**
 * Parses the configuration from the XML. For valid elements and attributes refer to {@link Element} and {@link
 * Attribute}
 *
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Gabriel Francisco &lt;gabfssilva@gmail.com&gt;
 * @author gustavonalle
 */
@Namespaces(value = {
	@Namespace(uri = "urn:infinispan:store:mongodb:*", root = "mongodb-store"),
	//@Namespace(uri = "urn:infinispan:store:mongodb:13.0.8", root = "mongodb-store"),
	@Namespace(root = "mongodb-store")
})
public class MongoDBCacheStoreConfigurationParser implements ConfigurationParser {

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder configurationBuilderHolder) {
      ConfigurationBuilder builder = configurationBuilderHolder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case MONGODB_STORE: {
            parseMongoDBStore(reader,
                    builder.persistence());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private void parseMongoDBStore(ConfigurationReader reader, PersistenceConfigurationBuilder persistenceConfigurationBuilder) {
      MongoDBStoreConfigurationBuilder builder = new MongoDBStoreConfigurationBuilder(persistenceConfigurationBuilder);

      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CONNECTION: {
               this.parseConnection(reader, builder);
               break;
            }
            default: {
               Parser.parseStoreElement(reader, builder);
            }
         }
      }
      persistenceConfigurationBuilder.addStore(builder);
   }

   private void parseConnection(ConfigurationReader reader, MongoDBStoreConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CONNECTION_URI: {
               builder.connectionURI(value);
               break;
            }
            case COLLECTION: {
               builder.collection(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

}
