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

package org.casia.cripac.isee.vpe.debug;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp;
import org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp.CommandSet;

/**
 * The CommandGenerator class is for simulating commands sent to the message handling application
 * through Kafka.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class CommandGenerator implements Serializable {
	
	private static final long serialVersionUID = -1221111574183021547L;
	private transient KafkaProducer<String, String> commandProducer;
	String brokers = null;
	
	public CommandGenerator(String brokers) {
		this.brokers = brokers;
		
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
	
	public void generatePresetCommand() {
		
		for (int i = 0; i < 5; ++i) {
			commandProducer.send(new ProducerRecord<String, String>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					"video123"));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					"video123");
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (int i = 0; i < 5; ++i) {
			commandProducer.send(new ProducerRecord<String, String>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					"video123"));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					"video123");

			commandProducer.send(new ProducerRecord<String, String>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.RECOG_ATTR_ONLY,
					"video123:1,2,3"));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.RECOG_ATTR_ONLY,
					"video123:1,2,3");
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (int i = 0; i < 15; ++i) {
			commandProducer.send(new ProducerRecord<String, String>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_ONLY,
					"video123"));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_ONLY,
					"video123");
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
