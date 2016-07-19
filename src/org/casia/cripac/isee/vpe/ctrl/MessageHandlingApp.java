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

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.LoggerFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class MessageHandlingApp extends SparkStreamingApp {
	
	public class CommandSet {
		public final static String TRACK_ONLY = "track-only";
		public final static String TRACK_AND_RECOG_ATTR = "track-and-recog-attr";
		public final static String RECOG_ATTR_ONLY = "recog-attr-only";
	}
	
	public final static String COMMAND_TOPIC = "command";
	public final static String APPLICATION_NAME = "MessageHandling";

	private static final long serialVersionUID = -942388332211825622L;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackingTaskProducerProperties;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams;
	private boolean verbose = false;
	
	public MessageHandlingApp(SystemPropertyCenter propertyCenter) {
		
		super();
		
		verbose = propertyCenter.verbose;
		
		topicsSet.add(COMMAND_TOPIC);
		
		trackingTaskProducerProperties = new Properties();
		trackingTaskProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		trackingTaskProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingTaskProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		
		//Create contexts.
		sparkConf = new SparkConf()
				.setAppName(APPLICATION_NAME);
		// Use fair sharing between jobs. 
		sparkConf = sparkConf
				.set("spark.scheduler.mode",
						propertyCenter.sparkSchedulerMode)
				.set("spark.shuffle.service.enabled",
						propertyCenter.sparkShuffleServiceEnabled)
				.set("spark.dynamicAllocation.enabled",
						propertyCenter.sparkDynamicAllocationEnabled)
				.set("spark.streaming.dynamicAllocation.enabled",
						propertyCenter.sparkStreamingDynamicAllocationEnabled)
				.set("spark.streaming.dynamicAllocation.minExecutors",
						propertyCenter.sparkStreamingDynamicAllocationMinExecutors)
				.set("spark.streaming.dynamicAllocation.maxExecutors",
						propertyCenter.sparkStreamingDynamicAllocationMaxExecutors)
				.set("spark.streaming.dynamicAllocation.debug",
						propertyCenter.sparkStreamingDynamicAllocationDebug)
				.set("spark.streaming.dynamicAllocation.delay.rounds",
						propertyCenter.sparkStreamingDynamicAllocationDelayRounds)
				.set("spark.executor.memory", propertyCenter.executorMem)
				.set("spark.rdd.compress", "true")
				.set("spark.storage.memoryFraction", "1");
		
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}
		
		commonKafkaParams = new HashMap<>();
		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		commonKafkaParams.put("group.id", "MessageHandlingApp");
		// Determine where the stream starts (default: largest)
		commonKafkaParams.put("auto.offset.reset", "smallest");
		commonKafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}
	
	private class ExecQueueBuilder {
		
		private String buf = "";
		
		public void addTask(String... topics) {
			if (buf.length() > 0 && topics.length > 0) {
				buf = buf.concat("|");
			}
			for (int i = 0; i < topics.length; ++i) {
				buf = buf.concat(topics[i]);
				if (i < topics.length - 1) {
					buf = buf.concat(",");
				}
			}
		}
		
		public String getQueue() {
			return buf;
		}
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		//Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> producerSingleton =
				new BroadcastSingleton<KafkaProducer<String, byte[]>>(
						new KafkaProducerFactory<>(trackingTaskProducerProperties),
						KafkaProducer.class);
		
		final BroadcastSingleton<Logger> loggerSingleton = new BroadcastSingleton<>(new LoggerFactory(), Logger.class);
		
		//Create an input DStream using Kafka.
		JavaPairInputDStream<String, byte[]> messageDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, topicsSet);
		
		//Handle the messages received from Kafka,
		messageDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, byte[]> messageRDD) throws Exception {

				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier =
						producerSingleton.getSupplier(new JavaSparkContext(messageRDD.context()));
				final ObjectSupplier<Logger> loggerSupplier = 
						loggerSingleton.getSupplier(new JavaSparkContext(messageRDD.context()));
				
				messageRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,byte[]>>>() {

					private static final long serialVersionUID = -4594138896649250346L;

					@Override
					public void call(Iterator<Tuple2<String, byte[]>> messages) throws Exception {
						
						while (messages.hasNext()) {
							Tuple2<String, byte[]> message = messages.next();
							
							ExecQueueBuilder execQueueBuilder = new ExecQueueBuilder();
							
							String command = message._1();
							byte[] dataQueue = message._2();
							
							if (verbose) {
								System.out.printf("MessageHandlingApp: received command \"%s\"\n", command);
								loggerSupplier.get().info("MessageHandlingApp: received command \"" + command + "\"");
							}
							
							switch (command) {
							case CommandSet.TRACK_ONLY:
								if (verbose) {
									System.out.printf(
											"MessageHandlingApp: sending to Kafka <%s>%s=...\n",
											PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
											execQueueBuilder.getQueue());
									loggerSupplier.get().info("MessageHandlingApp: sending to Kafka <" +
											PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC + ">" +
											execQueueBuilder.getQueue() + "...");
								}
								producerSupplier.get().send(
										new ProducerRecord<String, byte[]>(
												PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
												execQueueBuilder.getQueue(),
												dataQueue));
								break;
							case CommandSet.TRACK_AND_RECOG_ATTR:
								execQueueBuilder.addTask(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);

								if (verbose) {
									System.out.printf(
											"MessageHandlingApp: sending to Kafka <%s>%s=...\n",
											PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
											execQueueBuilder.getQueue());
									loggerSupplier.get().info("MessageHandlingApp: sending to Kafka <" +
											PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC + ">" +
											execQueueBuilder.getQueue() + "...");
								}
								producerSupplier.get().send(
										new ProducerRecord<String, byte[]>(
												PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC,
												execQueueBuilder.getQueue(),
												dataQueue));
								break;
							case CommandSet.RECOG_ATTR_ONLY:
								if (verbose) {
									System.out.printf(
											"Message handler: sending to Kafka <%s>%s=...\n",
											PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_TASK_TOPIC,
											execQueueBuilder.getQueue());
									loggerSupplier.get().info("MessageHandlingApp: sending to Kafka <" +
											PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_TASK_TOPIC + ">" +
											execQueueBuilder.getQueue() + "...");
								}
								producerSupplier.get().send(
										new ProducerRecord<String, byte[]>(
												PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_TASK_TOPIC,
												execQueueBuilder.getQueue(),
												dataQueue));
								break;
							default:
								System.err.println("MessageHandlingApp: Unsupported command!");
								loggerSupplier.get().error("MessageHandlingApp: Unsupported command!");
								break;
							}
						}
					}
				});
			}
		});
		
		return streamingContext;
	}

	public static void main(String[] args) throws URISyntaxException, ParserConfigurationException, SAXException {
		
		SystemPropertyCenter propertyCenter;
		if (args.length > 0) {
			propertyCenter = new SystemPropertyCenter(args);
		} else {
			propertyCenter = new SystemPropertyCenter();
		}

		TopicManager.checkTopics(propertyCenter);
		
		MessageHandlingApp messageHandlingApp = new MessageHandlingApp(propertyCenter);
		messageHandlingApp.initialize(propertyCenter);
		messageHandlingApp.start();
		messageHandlingApp.awaitTermination();
	}
}
