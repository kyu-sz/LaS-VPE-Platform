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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.KafkaSink;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class MessageHandlingApp extends SparkStreamingApp {
	
	public class CommandSet {
		public final static String TRACK_ONLY = "track-only";
		public final static String TRACK_AND_RECOG_ATTR = "track-and-recog-attr";
	}
	
	public final static String COMMAND_TOPIC = "command";
	public final static String APPLICATION_NAME = "MessageHandling";

	private static final long serialVersionUID = -942388332211825622L;
	private String kafkaBrokers;
	private String sparkMaster;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackingTaskProducerProperties;
	
	public MessageHandlingApp(String sparkMaster, String kafkaBrokers) {
		super();
		
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		System.out.println("Message handler: Kafka brokers: " + kafkaBrokers);
		
		topicsSet.add(COMMAND_TOPIC);
		
		trackingTaskProducerProperties = new Properties();
		trackingTaskProducerProperties.put("bootstrap.servers", kafkaBrokers);
		trackingTaskProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingTaskProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}
	
	private class ExecQueueBuilder {
		
		private String buf = "";
		
		public void addTask(String... topics) {
			for (int i = 0; i < topics.length; ++i) {
				buf = buf.concat(topics[i] + ((i == topics.length - 1) ? "|" : ","));
			}
		}
		
		public String getQueue() {
			return buf;
		}
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		SparkConf sparkConf = new SparkConf()
				.setMaster(sparkMaster)
				.setAppName(APPLICATION_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		//Create KafkaSink for Spark Streaming to output to Kafka.
		final Broadcast<KafkaSink<String, String>> broadcastKafkaSink =
				sparkContext.broadcast(new KafkaSink<String, String>(trackingTaskProducerProperties));
		
		//Create an input DStream using Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		kafkaParams.put("group.id", "0");
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		//Handle the messages received from Kafka,
		messagesDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, String> messagesRDD) throws Exception {
				
				messagesRDD.foreach(new VoidFunction<Tuple2<String, String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, String> message) throws Exception {
						
						ExecQueueBuilder execQueueBuilder = new ExecQueueBuilder();
						
						String command = message._1();
						String params = message._2();
						switch (command) {
						case CommandSet.TRACK_ONLY:
							System.out.printf(
									"Message handler: sending to Kafka <%s>%s=%s\n",
									PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
									execQueueBuilder.getQueue(),
									params);
							broadcastKafkaSink.value().send(
									new ProducerRecord<String, String>(
											PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
											execQueueBuilder.getQueue(),
											params));
							break;
						case CommandSet.TRACK_AND_RECOG_ATTR:
							execQueueBuilder.addTask(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);

							System.out.printf(
									"Message handler: sending to Kafka <%s>%s=%s\n",
									PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
									execQueueBuilder.getQueue(),
									params);
							broadcastKafkaSink.value().send(
									new ProducerRecord<String, String>(
											PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
											execQueueBuilder.getQueue(),
											params));
							break;
						}
					}
				});
			}
		});
		
		return streamingContext;
	}
}
