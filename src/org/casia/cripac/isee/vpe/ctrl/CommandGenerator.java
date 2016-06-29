/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.casia.cripac.isee.vpe.ctrl;

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
