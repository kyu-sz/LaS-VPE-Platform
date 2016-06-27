package org.isee.vpe.common;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A smart wrapper for KafkaProducer.
 * Initialize a producer just before using it, like lazy evaluation in Scala.
 * KafkaProducer is not serializable, so it cannot be broadcast to Spark Streaming executors.
 * But this wrapper can be serialized, thus solving this problem.
 * To use it with Spark Streaming, first use SparkContext to create a broadcast variable of KafkaSink:
 * <pre>
 * {@code
 * Broadcast<KafkaSink<K, V>> broadcastKafkaSink = sparkContext.broadcast(new KafkaSink<K, V>(config))
 * }
 * </pre>
 * Then in an executor, get a KafkaSink from the broadcast and use it just like using KafkaProducer:
 * <pre>
 * {@code
 * broadcastKafkaSink.value().send(topic, value);
 * }
 * </pre>
 * @author ken
 *
 * @param <K> Type of the key in a Kafka message.
 * @param <V> Type of the value in a Kafka message.
 */
public class KafkaSink<K, V> implements Serializable {
	private static final long serialVersionUID = -7565726994857167434L;

	/**
	 * Lazy-evaluated Kafka producer.
	 */
	private transient KafkaProducer<K, V> producer = null;
	
	/**
	 * Configuration for Kafka producer.
	 */
	private Properties config;
	
	/**
	 * Constructor inputting a configuration for initializing KafkaProducer.
	 * The producer is not initialized immediately here, but lazy-evaluated somewhere else.
	 * @param config The configuration for KafkaProducer.
	 */
	public KafkaSink(Properties config) {
		this.config = config;
	}
	
	/**
	 * Send a record to Kafka with value specified only.
	 * Kafka producer might be evaluated here.
	 * @param record The record to send.
	 * @return Future result of the meta data of the record.
	 */
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		if (producer == null) {
			producer = new KafkaProducer<K, V>(config);
		}
		
		return producer.send(record);
	}
	
	@Override
	protected void finalize() throws Throwable {
		if (producer != null)
			producer.close();
		
		super.finalize();
	}
};
