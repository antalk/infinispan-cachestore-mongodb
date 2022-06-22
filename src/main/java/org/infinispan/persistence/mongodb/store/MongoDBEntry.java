package org.infinispan.persistence.mongodb.store;

import org.infinispan.commons.marshall.Marshaller;

/**
 * This is a representation of a MongoDBStore entry. <br/>
 * This class IS NOT persisted to MongoDB, only its byte array and expiryTime
 * attributes.
 *
 * @author Gabriel Francisco &lt;gabfssilva@gmail.com&gt;
 */
public class MongoDBEntry<K, V> {
	private K key;
	private V value;

	private byte[] keyBytes;
	private byte[] valueBytes;
	private byte[] metadataBytes;

	private ExpirationUnit expiration;

	public MongoDBEntry(byte[] keyBytes, byte[] valueBytes, byte[] metadataBytes, ExpirationUnit expiration) {
		this.keyBytes = keyBytes;
		this.valueBytes = valueBytes;
		this.metadataBytes = metadataBytes;
		this.expiration = expiration;
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public byte[] getKeyBytes() {
		return keyBytes;
	}

	public void setKeyBytes(byte[] keyBytes) {
		this.keyBytes = keyBytes;
	}

	public byte[] getValueBytes() {
		return valueBytes;
	}

	public void setValueBytes(byte[] valueBytes) {
		this.valueBytes = valueBytes;
	}

	public byte[] getMetadataBytes() {
		return metadataBytes;
	}

	public void setMetadataBytes(byte[] metadataBytes) {
		this.metadataBytes = metadataBytes;
	}

	public ExpirationUnit getExpiryTime() {
		return expiration;
	}

	public void setExpiryTime(ExpirationUnit expiration) {
		this.expiration = expiration;
	}

	public K getKey(Marshaller marshaller) {
		try {
			return (K) marshaller.objectFromByteBuffer(keyBytes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public V getValue(Marshaller marshaller) {
		try {
			return (V) marshaller.objectFromByteBuffer(valueBytes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static class ExpirationUnit {

		private final Long ttl;
		private final Long created;
		private final Long lastAccess;

		public ExpirationUnit(Long ttl, Long lastAccess, Long created) {
			this.ttl = ttl;
			this.created = created;
			this.lastAccess = lastAccess;

		}

		public Long getTtl() {
			return ttl;
		}

		public Long getCreated() {
			return created;
		}

		public Long getLastAccess() {
			return lastAccess;
		}

	}

	public static class Builder<K, V> {
		private byte[] keyBytes;
		private byte[] valueBytes;
		private byte[] metadataBytes;

		private ExpirationUnit expiryTime;

		private Builder() {
		}

		public Builder<K, V> keyBytes(byte[] keyBytes) {
			this.keyBytes = keyBytes;
			return this;
		}

		public Builder<K, V> valueBytes(byte[] valueBytes) {
			this.valueBytes = valueBytes;
			return this;
		}

		public Builder<K, V> metadataBytes(byte[] metadataBytes) {
			this.metadataBytes = metadataBytes;
			return this;
		}

		public Builder<K, V> expiryTime(ExpirationUnit expiryTime) {
			this.expiryTime = expiryTime;
			return this;
		}

		public MongoDBEntry<K, V> create() {
			return new MongoDBEntry<>(keyBytes, valueBytes, metadataBytes, expiryTime);
		}
	}
}
