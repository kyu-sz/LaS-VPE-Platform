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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The MessageHandlingApp class is a Spark Streaming application responsible for
 * receiving commands from sources like web-UI, then producing appropriate command messages
 * and sending to command-defined starting application.
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class MessageHandlingApp extends SparkStreamingApp {
	
	/**
	 * This class stores possible commands and the String expressions of them.
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public static class CommandSet {
		public final static String TRACK_ONLY = "track-only";
		public final static String TRACK_AND_RECOG_ATTR = "track-and-recog-attr";
		public final static String RECOG_ATTR_ONLY = "recog-attr-only";
	}
	
	public final static String COMMAND_TOPIC = "command";
	public final static String APPLICATION_NAME = "MessageHandling";

	private static final long serialVersionUID = -942388332211825622L;
//	private HashSet<String> topicSet = new HashSet<>();
	private Map<String, Integer> topicPartitions = new HashMap<>();
	private Properties trackingTaskProducerProperties;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams;
	private boolean verbose = false;
	
	String messageListenerAddr;
	int messageListenerPort;
	
	/**
	 * The constructor method. It sets the configurations, but does not start the contexts.
	 * @param propertyCenter	The propertyCenter stores all the available configurations.
	 */
	public MessageHandlingApp(SystemPropertyCenter propertyCenter) {
		
		super();
		
		verbose = propertyCenter.verbose;
		
		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;
		
//		topicSet.add(COMMAND_TOPIC);
		topicPartitions.put(COMMAND_TOPIC, propertyCenter.kafkaPartitions);
		
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
				.set("spark.storage.memoryFraction", "1")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");
		
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}
		
		commonKafkaParams = new HashMap<>();
		System.out.println("MessageHandlingApp: metadata.broker.list=" + propertyCenter.kafkaBrokers);
		commonKafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		commonKafkaParams.put("group.id", "MessageHandlingApp" + UUID.randomUUID());
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

		final BroadcastSingleton<SynthesizedLogger> loggerSingleton =
				new BroadcastSingleton<>(
						new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort),
						SynthesizedLogger.class);
		
		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by Zookeeper.
		 * TODO Create multiple input streams and unite them together for higher receiving speed.
		 * @link http://spark.apache.org/docs/latest/streaming-programming-guide.html#level-of-parallelism-in-data-receiving
		 * TODO Find ways to robustly make use of createDirectStream.
		 */
		JavaPairReceiverInputDStream<String, byte[]> messageDStream = KafkaUtils.createStream(
				streamingContext,
				String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
				commonKafkaParams,
				topicPartitions, 
				StorageLevel.MEMORY_AND_DISK_SER());
		
//		//Create an input DStream using Kafka.
//		JavaPairInputDStream<String, byte[]> messageDStream =
//				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
//				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, topicSet);
		
		//Handle the messages received from Kafka,
		messageDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, byte[]> messageRDD) throws Exception {

				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier =
						producerSingleton.getSupplier(new JavaSparkContext(messageRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = 
						loggerSingleton.getSupplier(new JavaSparkContext(messageRDD.context()));
				
				System.out.println("RDD count: " + messageRDD.count() + " partitions:" + messageRDD.getNumPartitions()
				+ " " + messageRDD.toString());
				
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
								loggerSupplier.get().info("MessageHandlingApp: received command \"" + command + "\"");
							}
							
							switch (command) {
							case CommandSet.TRACK_ONLY:
								if (verbose) {
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
