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

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The MessageHandlingApp class is a Spark Streaming application responsible for
 * receiving commands from sources like web-UI, then producing appropriate
 * command messages and sending to command-defined starting application.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class MessageHandlingApp extends SparkStreamingApp {

	/**
	 * This class stores possible commands and the String expressions of them.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public static class CommandSet {
		public final static String TRACK_ONLY = "track-only";
		public final static String TRACK_AND_RECOG_ATTR = "track-and-recog-attr";
		public final static String RECOG_ATTR_ONLY = "recog-attr-only";
	}

	private static final long serialVersionUID = -942388332211825622L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "MessageHandling";
	public static final String COMMAND_TOPIC = "command";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * whole system, the TopicManager can help register the topics this
	 * application needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(COMMAND_TOPIC);
	}

	// private HashSet<String> topicSet = new HashSet<>();
	private Map<String, Integer> topicPartitions = new HashMap<>();
	private Properties trackingTaskProducerProperties;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams;
	private boolean verbose = false;
	private String messageListenerAddr;
	private int messageListenerPort;
	private int numRecvStreams;

	/**
	 * The constructor method. It sets the configurations, but does not start
	 * the contexts.
	 * 
	 * @param propertyCenter
	 *            The propertyCenter stores all the available configurations.
	 */
	public MessageHandlingApp(SystemPropertyCenter propertyCenter) {

		super();

		verbose = propertyCenter.verbose;

		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		// topicSet.add(COMMAND_TOPIC);
		topicPartitions.put(COMMAND_TOPIC, propertyCenter.kafkaPartitions);

		trackingTaskProducerProperties = new Properties();
		trackingTaskProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		trackingTaskProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		trackingTaskProducerProperties.put("value.serializer",
				"org.apache.kafka.common.serialization.ByteArraySerializer");

		// Create contexts.
		sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");

		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf.setMaster(propertyCenter.sparkMaster).set("deploy.mode",
					propertyCenter.sparkDeployMode);
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

	public static class UnsupportedCommandException extends Exception {

		private static final long serialVersionUID = -940732652485656739L;

	}

	private ExecutionPlan createPlanByCommand(String cmd, byte[] data)
			throws UnsupportedCommandException, ClassNotFoundException, IOException {
		ExecutionPlan plan = null;

		switch (cmd) {
		case CommandSet.TRACK_ONLY:
			plan = new ExecutionPlan(1);
			plan.addNode(PedestrianTrackingApp.PEDESTRIAN_TRACKING_JOB_TOPIC_TOPIC,
					(Serializable) SerializationHelper.deserialized(data));
			break;
		case CommandSet.TRACK_AND_RECOG_ATTR:
			plan = new ExecutionPlan(2);
			int trackingID = plan.addNode(PedestrianTrackingApp.PEDESTRIAN_TRACKING_JOB_TOPIC_TOPIC,
					(Serializable) SerializationHelper.deserialized(data));
			int attrRecogID = plan.addNode(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC);
			plan.linkNodes(trackingID, attrRecogID);
			break;
		case CommandSet.RECOG_ATTR_ONLY:
			plan = new ExecutionPlan(1);
			plan.addNode(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_JOB_TOPIC,
					(Serializable) SerializationHelper.deserialized(data));
			break;
		default:
			throw new UnsupportedCommandException();
		}

		return plan;
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		// Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> producerSingleton = new BroadcastSingleton<KafkaProducer<String, byte[]>>(
				new KafkaProducerFactory<>(trackingTaskProducerProperties), KafkaProducer.class);

		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort), SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 * TODO Fix problem "numRecords must not be negative" when using
		 * createDirectStream.
		 */
		List<JavaPairDStream<String, byte[]>> parCmdStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parCmdStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class, StringDecoder.class,
					DefaultDecoder.class, commonKafkaParams, topicPartitions, StorageLevel.MEMORY_AND_DISK_SER()));
		}
		JavaPairDStream<String, byte[]> cmdDStream = streamingContext.union(parCmdStreams.get(0),
				parCmdStreams.subList(1, parCmdStreams.size()));
		// //Create an input DStream using Kafka.
		// JavaPairInputDStream<String, byte[]> messageDStream =
		// KafkaUtils.createDirectStream(streamingContext, String.class,
		// byte[].class,
		// StringDecoder.class, DefaultDecoder.class, commonKafkaParams,
		// topicSet);

		// Handle the messages received from Kafka,
		cmdDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, byte[]> cmdRDD) throws Exception {

				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = producerSingleton
						.getSupplier(new JavaSparkContext(cmdRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(cmdRDD.context()));

				if (verbose) {
					System.out.println(APP_NAME + ": Reading RDD...");
					System.out.println(APP_NAME + ": RDD count=" + cmdRDD.count() + " partitions="
							+ cmdRDD.getNumPartitions() + " " + cmdRDD.toString());
					System.out.println(APP_NAME + ": Starting foreachPartition...");
				}

				cmdRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");

				cmdRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, byte[]>>>() {

					private static final long serialVersionUID = -4594138896649250346L;

					@Override
					public void call(Iterator<Tuple2<String, byte[]>> cmdMsg) throws Exception {

						int msgCnt = 0; // For diagnose.

						while (cmdMsg.hasNext()) {
							// Get a next command message.
							Tuple2<String, byte[]> message = cmdMsg.next();
							++msgCnt;

							UUID taskID = UUID.randomUUID();

							String cmd = message._1();
							byte[] data = message._2();

							if (verbose) {
								loggerSupplier.get().info(APP_NAME + ": Received command \"" + cmd + "\"");
							}

							ExecutionPlan plan = createPlanByCommand(cmd, data);

							Set<Integer> startableNodes = plan.getStartableNodes();
							for (int nodeID : startableNodes) {
								producerSupplier.get()
										.send(new ProducerRecord<String, byte[]>(plan.getInputTopicName(nodeID),
												taskID.toString(),
												SerializationHelper.serialize(new TaskData(nodeID, plan))));
							}

						}

						if (verbose) {
							loggerSupplier.get().info(APP_NAME + ": Received " + msgCnt + " commands.");
						}
					}
				});
			}
		});

		return streamingContext;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.common.SparkStreamingApp#getAppName()
	 */
	@Override
	public String getAppName() {
		return APP_NAME;
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
