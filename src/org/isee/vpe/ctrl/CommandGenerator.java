package org.isee.vpe.ctrl;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The CommandGenerator class is for simulating commands sent to the message handling application
 * through Kafka.
 * TODO The command format should be further considered.
 * @author Ken Yu, ISEE, 2016
 *
 */
public class CommandGenerator implements Serializable {
	
	private static final long serialVersionUID = -1221111574183021547L;
	private transient KafkaProducer<String, String> commandProducer;
	String brokers = null;
	String topic = null;
	
	public CommandGenerator(String brokers) {
		this.brokers = brokers;
		this.topic = MessageHandlingApp.COMMAND_TOPIC;
		
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
		for (int i = 0; i < 6; ++i) {
			commandProducer.send(new ProducerRecord<String, String>(topic, "Track", "video123"));
			System.out.println("Command producer: sent to kafka <" + topic + ">" + "Track video123");
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
