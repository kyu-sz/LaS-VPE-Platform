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

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.StreamingContext;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp;
import org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp.CommandSet;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;

/**
 * The CommandGenerator class is for simulating commands sent to the message handling application
 * through Kafka.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class CommandGeneratingApp implements Serializable {
	
	private static final long serialVersionUID = -1221111574183021547L;
	private transient KafkaProducer<String, byte[]> commandProducer;
	
	public static final String APPLICATION_NAME = "CommandGenerating";
	
	public CommandGeneratingApp(SystemPropertyCenter propertyCenter) {
		Properties commandProducerProperties = new Properties();
		commandProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		commandProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		commandProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		commandProducer = new KafkaProducer<String, byte[]>(commandProducerProperties);
	}
	
	@Override
	protected void finalize() throws Throwable {
		commandProducer.close();
		super.finalize();
	}
	
	public void generatePresetCommand() throws IOException {
		
		String trackArg = "video123";
		byte[] trackArgBytes =
				ByteArrayFactory.compress(
						ByteArrayFactory.appendLengthToHead(
								ByteArrayFactory.getByteArray(trackArg)));

		String attrRecogArg = "video123:12";
		byte[] attrRecogArgBytes = 
				ByteArrayFactory.compress(
						ByteArrayFactory.appendLengthToHead(
								ByteArrayFactory.getByteArray(attrRecogArg)));
		
		for (int i = 0; i < 3; ++i) {
			commandProducer.send(new ProducerRecord<String, byte[]>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					trackArgBytes));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					trackArg);
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (int i = 0; i < 3; ++i) {
			commandProducer.send(new ProducerRecord<String, byte[]>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					trackArgBytes));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_AND_RECOG_ATTR,
					trackArg);

			commandProducer.send(new ProducerRecord<String, byte[]>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.RECOG_ATTR_ONLY,
					attrRecogArgBytes));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.RECOG_ATTR_ONLY,
					attrRecogArg);
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		for (int i = 0; i < 3; ++i) {
			commandProducer.send(new ProducerRecord<String, byte[]>(
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_ONLY,
					trackArgBytes));
			System.out.printf(
					"Command producer: sent to kafka <%s>%s=%s\n",
					MessageHandlingApp.COMMAND_TOPIC,
					CommandSet.TRACK_ONLY,
					trackArg);
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {

		SystemPropertyCenter propertyCenter;
		if (args.length > 0) {
			propertyCenter = new SystemPropertyCenter(args);
		} else {
			propertyCenter = new SystemPropertyCenter();
		}

		TopicManager.checkTopics(propertyCenter);
		
		CommandGeneratingApp app = new CommandGeneratingApp(propertyCenter);
		app.generatePresetCommand();
		
//		SparkContext context = new SparkContext(new SparkConf().setAppName("CommandGeneratingApp"));
//		StreamingContext streamingContext = new StreamingContext(context, new Duration(10));
//		streamingContext.start();
//		streamingContext.stop(true);
	}
}
