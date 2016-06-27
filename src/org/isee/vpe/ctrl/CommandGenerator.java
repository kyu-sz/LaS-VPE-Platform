package org.isee.vpe.ctrl;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CommandGenerator implements Serializable {
	
	private static final long serialVersionUID = -1221111574183021547L;
	private transient KafkaProducer<String, String> commandProducer;
	String brokers = null;
	String topic = null;
	
	public CommandGenerator(String brokers) {
		this.brokers = brokers;
		this.topic = "command";
		
		Properties commandProducerProperties = new Properties();
		commandProducerProperties.put("bootstrap.servers", brokers);
		commandProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		commandProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		commandProducer = new KafkaProducer<String, String>(commandProducerProperties);
	}
	
	@Override
	protected void finalize() throws Throwable {
		commandProducer.close();
		super.finalize();
	}
	
	void generatePresetCommand() {
		commandProducer.send(new ProducerRecord<String, String>(topic, "Track video1 for ken"));
	}
}
