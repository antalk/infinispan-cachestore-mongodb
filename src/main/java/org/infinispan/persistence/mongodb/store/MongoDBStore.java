package org.infinispan.persistence.mongodb.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.mongodb.cache.MongoDBCache;
import org.infinispan.persistence.mongodb.cache.MongoDBCacheImpl;
import org.infinispan.persistence.mongodb.configuration.MongoDBStoreConfiguration;
import org.infinispan.persistence.mongodb.store.MongoDBEntry.ExpirationUnit;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.internal.operators.flowable.FlowableAll;
import net.jcip.annotations.ThreadSafe;

/**
 * AdvancedLoadWriteStore implementation based on MongoDB. <br/>
 * This class is fully thread safe
 *
 * @param <K>
 * @param <V>
 * @author Gabriel Francisco &lt;gabfssilva@gmail.com&gt;
 */
@ThreadSafe
@ConfiguredBy(MongoDBStoreConfiguration.class)
public class MongoDBStore<K, V> implements NonBlockingStore<K, V> {
	
	private static Logger logger = LoggerFactory.getLogger(MongoDBStore.class);
	
	
   private InitializationContext context;

   private MongoDBCache<K, V> cache;
   private MongoDBStoreConfiguration configuration;
   
   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SHAREABLE);
   }
   
   	@Override
	public CompletionStage<Void> start(InitializationContext ctx) {
   		return ctx.getBlockingManager().runBlocking(() -> {
   			this.context = ctx;
   			this.configuration = ctx.getConfiguration();
   			try {
   				cache = new MongoDBCacheImpl<>(configuration, ctx.getTimeService());
   				
   				cache.start();
	   		      if (configuration.purgeOnStartup()) {
	   		         cache.clear();
	   		      }
   				
   			} catch (Exception e) {
   				throw new PersistenceException(e);
   			}
         }, "mongodbstore-start");
	}
   	
   	@Override
   	public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
   		logger.debug("publishing entries for cache {} with filter {}", cache, filter);
   		
   		/**
   		
   		return Flowable.defer(() -> {
   			UnicastProcessor<MarshallableEntry<K, V>> unicastProcessor = UnicastProcessor.create();
   			blockingManager.runBlocking(() -> {

   			
		   		//A while loop since we have to hit the db again for paging.
		        boolean shouldContinue = true;
		        byte[] id = null;
		        while (shouldContinue) {
		           final List<MongoDBEntry<K, V>> entries = cache.getPagedEntries(id);
		           shouldContinue = !entries.isEmpty();
		           if (shouldContinue) {
			             for (final MongoDBEntry<K, V> entry : entries) {
			                final K marshalledKey = (K) toObject(entry.getKeyBytes());
			                if (filter == null || filter.test(marshalledKey)) {
			                   final MarshallableEntry<K, V> marshalledEntry = getMarshalledEntry(entry);
			                   if (marshalledEntry != null) {
			                	   unicastProcessor.onNext(marshalledEntry);
			                   }
			                }
			             }
			             //get last key so we can get more entries.
			             id = entries.get(entries.size() - 1).getKeyBytes();
		           }
		        }
		        unicastProcessor.onComplete();
   			},"mongodbstore-publish");
   			return unicastProcessor;
   		});
   		**/
   		
   		List<MarshallableEntry<K, V>> items = new ArrayList<>();
   		
   	//A while loop since we have to hit the db again for paging.
        boolean shouldContinue = true;
        byte[] id = null;
        while (shouldContinue) {
           final List<MongoDBEntry<K, V>> entries = cache.getPagedEntries(id);
           shouldContinue = !entries.isEmpty();
           if (shouldContinue) {
	             for (final MongoDBEntry<K, V> entry : entries) {
	                final K marshalledKey = (K) toObject(entry.getKeyBytes());
	                if (filter == null || filter.test(marshalledKey)) {
	                   final MarshallableEntry<K, V> marshalledEntry = getMarshalledEntry(entry);
	                   if (marshalledEntry != null) {
	                	   items.add(marshalledEntry);
	                   }
	                }
	             }
	             //get last key so we can get more entries.
	             id = entries.get(entries.size() - 1).getKeyBytes();
           }
        }
   		
   		return new FlowableAll<MarshallableEntry<K, V>>(Flowable.fromIterable(items), null).source();
   	}
   	
   
   	/*
   @Override
   public void process(final KeyFilter<? super K> filter, final CacheLoaderTask<K, V> task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
      final TaskContextImpl taskContext = new TaskContextImpl();

      //A while loop since we have to hit the db again for paging.
      boolean shouldContinue = true;
      byte[] id = null;
      while (shouldContinue) {
         final List<MongoDBEntry<K, V>> entries = cache.getPagedEntries(id);
         shouldContinue = !entries.isEmpty();
         if (taskContext.isStopped()) {
            break;
         }
         if (shouldContinue) {
            eacs.submit(() -> {
               for (final MongoDBEntry<K, V> entry : entries) {
                  if (taskContext.isStopped()) {
                     break;
                  }
                  final K marshalledKey = (K) toObject(entry.getKeyBytes());
                  if (filter == null || filter.accept(marshalledKey)) {
                     final MarshallableEntry<K, V> marshalledEntry = getMarshalledEntry(entry);
                     if (marshalledEntry != null) {
                        task.processEntry(marshalledEntry, taskContext);
                     }
                  }
               }
               return null;
            });
            //get last key so we can get more entries.
            id = entries.get(entries.size() - 1).getKeyBytes();
         }
      }
      eacs.waitUntilAllCompleted();
      if (eacs.isExceptionThrown()) {
         throw new PersistenceException("Execution exception!", eacs.getFirstException());
      }

   }*/

   @Override
   public CompletionStage<Long> size(IntSet segments) {
	   logger.debug("getSize for cache {}", cache);
	   return CompletableFuture.supplyAsync(() ->  Long.valueOf(cache.size()));
   }

   @Override
	public CompletionStage<Void> clear() {
	   cache.clear();
	   return CompletableFuture.completedFuture(null);
	}
  
   

   	@Override
   	public Publisher<MarshallableEntry<K, V>> purgeExpired() {
   		logger.debug("purge expired items for cache {}", cache);
   		
   		List<MarshallableEntry<K, V>> purgedItems = new ArrayList<>();

   		
   		byte[] lastKey = null;
        boolean shouldContinue = true;
        while (shouldContinue) {
           List<MongoDBEntry<K, V>> expired = cache.removeExpiredData(lastKey);
           expired.forEach(kvMongoDBEntry -> purgedItems.add(getMarshalledEntry(kvMongoDBEntry)));
           shouldContinue = !expired.isEmpty();
           if (shouldContinue) {
              lastKey = expired.get(expired.size() - 1).getKeyBytes();
           }
        }
   		return new FlowableAll<MarshallableEntry<K, V>>(Flowable.fromIterable(purgedItems), null).source();

   }

   	
   	@Override
	public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
   		logger.debug("write entry {} for cache {}", entry, cache);
   		
   		MongoDBEntry.Builder<K, V> mongoDBEntryBuilder = MongoDBEntry.builder();
			
			mongoDBEntryBuilder
	              .keyBytes(toByteArray(entry.getKey()))
	              .valueBytes(toByteArray(entry.getValue()))
	              .expiryTime(new ExpirationUnit(entry.expiryTime(),entry.lastUsed(),entry.created()));

			if (entry.getMetadata() != null) {
				mongoDBEntryBuilder.metadataBytes(toByteArray(entry.getMetadata()));
			}
			
			
			MongoDBEntry<K, V> mongoDBEntry = mongoDBEntryBuilder.create();

			cache.put(mongoDBEntry);
   		
   		
   		return CompletableFuture.completedFuture(null);
   		
	}


   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
	   logger.debug("delete key {} for cache {}", key, cache);
	   return CompletableFuture.supplyAsync(() -> cache.remove(toByteArray(key)));
   }


   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
	   logger.debug("load key {} for cache {}", key, cache);
	   return CompletableFuture.supplyAsync(()-> load(key, false));
   }

   private MarshallableEntry<K, V> load(Object key, boolean binaryData) {
      MongoDBEntry<K, V> mongoDBEntry = cache.get(toByteArray(key));

      if (mongoDBEntry == null) {
         return null;
      }
      V v = mongoDBEntry.getValue(marshaller());

      Metadata metadata = null;
      if (mongoDBEntry.getMetadataBytes() != null) {
    	  metadata = (Metadata) toObject(mongoDBEntry.getMetadataBytes());
      }

      MarshallableEntry<K, V> entry = (MarshallableEntry<K, V>) 
    		  context.getMarshallableEntryFactory().create(key, v, metadata, null, mongoDBEntry.getExpiryTime().getCreated(),mongoDBEntry.getExpiryTime().getLastAccess());

      if (isExpired(mongoDBEntry)) {
         return null;
      }

      return entry;
   }

   private MarshallableEntry<K, V> getMarshalledEntry(MongoDBEntry<K, V> mongoDBEntry) {

      if (mongoDBEntry == null) {
         return null;
      }

      K k = mongoDBEntry.getKey(marshaller());
      V v = mongoDBEntry.getValue(marshaller());

      
      Metadata metadata = null;
      if (mongoDBEntry.getMetadataBytes() != null) {
    	  metadata = (Metadata) toObject(mongoDBEntry.getMetadataBytes());
      }

      MarshallableEntry<K, V> result = (MarshallableEntry<K, V>) context.getMarshallableEntryFactory().create(k, v, metadata, null, mongoDBEntry.getExpiryTime().getCreated(), mongoDBEntry.getExpiryTime().getLastAccess());
      return result;
   }



   @Override
   public CompletionStage<Void> stop() {
	   cache.stop();
	   return CompletableFuture.completedFuture(null);
   }

   private boolean isExpired(MongoDBEntry result) {
	  if (result.getExpiryTime().getTtl() > 0 ) {
		  return result.getExpiryTime().getTtl().compareTo(context.getTimeService().wallClockTime()) < 0;
	  }
	  return false;
   }

   private Object toObject(byte[] bytes) {
      try {
         return marshaller().objectFromByteBuffer(bytes);
      } catch (IOException | ClassNotFoundException e) {
         e.printStackTrace();
      } 
      return null;
   }

   private byte[] toByteArray(Object obj) {
      try {
         return marshaller().objectToByteBuffer(obj);
      } catch (IOException | InterruptedException e) {
         e.printStackTrace();
      } 
      return null;
   }

   private Marshaller marshaller() {
      return context.getPersistenceMarshaller();

   }

   public MongoDBCache<K, V> getCache() {
      return cache;
   }
}
