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

package org.casia.cripac.isee.vpe.alg;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.ctrl.TaskData;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The PedestrianTrackingApp class takes in video URLs from Kafka, then process
 * the videos with pedestrian tracking algorithms, and finally push the tracking
 * results back to Kafka.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianTrackingApp extends SparkStreamingApp {

	private static final long serialVersionUID = 3104859533881615664L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "PedestrianTracking";
	public static final String PEDESTRIAN_TRACKING_JOB_TOPIC_TOPIC = "tracking-task";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * whole system, the TopicManager can help register the topics this
	 * application needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(PEDESTRIAN_TRACKING_JOB_TOPIC_TOPIC);
	}

	// private HashSet<String> taskTopicsSet = new HashSet<>();
	private Map<String, Integer> taskTopicPartitions = new HashMap<>();
	private Properties trackProducerProperties = new Properties();
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private boolean verbose = false;
	private String messageListenerAddr;
	private int messageListenerPort;
	private int numRecvStreams;

	public PedestrianTrackingApp(SystemPropertyCenter propertyCenter) {
		super();

		verbose = propertyCenter.verbose;

		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		// taskTopicsSet.add(PEDESTRIAN_TRACKING_TASK_TOPIC);
		taskTopicPartitions.put(PEDESTRIAN_TRACKING_JOB_TOPIC_TOPIC, propertyCenter.kafkaPartitions);

		trackProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		trackProducerProperties.put("producer.type", "sync");
		trackProducerProperties.put("request.required.acks", "1");
		trackProducerProperties.put("compression.codec", "gzip");
		trackProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		trackProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		// Create contexts.
		sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");

		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf.setMaster(propertyCenter.sparkMaster).set("deploy.mode",
					propertyCenter.sparkDeployMode);
		}

		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		commonKafkaParams.put("group.id", "PedestrianTrackingApp" + UUID.randomUUID());
		commonKafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		// Determine where the stream starts (default: largest)
		commonKafkaParams.put("auto.offset.reset", "smallest");
		commonKafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		// Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		// Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> producerSingleton = new BroadcastSingleton<KafkaProducer<String, byte[]>>(
				new KafkaProducerFactory<>(trackProducerProperties), KafkaProducer.class);
		// Create ResourceSink for any other unserializable components.
		final BroadcastSingleton<PedestrianTracker> trackerSingleton = new BroadcastSingleton<PedestrianTracker>(
				new ObjectFactory<PedestrianTracker>() {

					private static final long serialVersionUID = -3454317350293711609L;

					@Override
					public PedestrianTracker getObject() {
						return new FakePedestrianTracker();
					}
				}, PedestrianTracker.class);
		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort), SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 */
		List<JavaPairDStream<String, byte[]>> parJobStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parJobStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class,
					StringDecoder.class, DefaultDecoder.class, commonKafkaParams, taskTopicPartitions,
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		JavaPairDStream<String, byte[]> jobStream = streamingContext.union(parJobStreams.get(0),
				parJobStreams.subList(1, parJobStreams.size()));
		// //Retrieve messages from Kafka.
		// JavaPairInputDStream<String, byte[]> taskDStream =
		// KafkaUtils.createDirectStream(streamingContext, String.class,
		// byte[].class,
		// StringDecoder.class, DefaultDecoder.class, commonKafkaParams,
		// taskTopicsSet);

		jobStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -6015951200762719085L;

			@Override
			public void call(JavaPairRDD<String, byte[]> taskRDD) throws Exception {

				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = producerSingleton
						.getSupplier(new JavaSparkContext(taskRDD.context()));
				final ObjectSupplier<PedestrianTracker> trackerSupplier = trackerSingleton
						.getSupplier(new JavaSparkContext(taskRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(taskRDD.context()));

				taskRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				taskRDD.foreach(new VoidFunction<Tuple2<String, byte[]>>() {

					private static final long serialVersionUID = 955383087048954689L;

					@Override
					public void call(Tuple2<String, byte[]> task) throws Exception {
						// Get the task data.
						TaskData taskData = (TaskData) SerializationHelper.deserialized(task._2());
						// Get the URL of the video to process from the
						// execution data of this node.
						String videoURL = (String) taskData.executionPlan.getExecutionData(taskData.currentNodeID);
						// Get the IDs of successor nodes.
						Set<Integer> successorIDs = taskData.executionPlan.getSuccessors(taskData.currentNodeID);
						// Mark the current node as executed in advance.
						taskData.executionPlan.markExecuted(taskData.currentNodeID);
						// Do tracking.
						Track[] tracks = trackerSupplier.get().track(videoURL);
						// Send tracks.
						for (Track track : tracks) {
							// Stored the track in the task data, which can be
							// cyclic utilized.
							taskData.predecessorResult = track;
							// Send to all the successor nodes.
							for (int successorID : successorIDs) {
								taskData.currentNodeID = successorID;
								String topic = taskData.executionPlan.getInputTopicName(successorID);

								if (verbose) {
									loggerSupplier.get().info(
											"PedestrianTrackingApp: Sending to Kafka <" + topic + "> :" + "A track");
								}
								producerSupplier.get().send(new ProducerRecord<String, byte[]>(topic, task._1(),
										SerializationHelper.serialize(taskData)));
							}

							// Always send to the meta data saving application.
							if (verbose) {
								loggerSupplier.get().info("PedestrianTrackingApp: Sending to Kafka: <"
										+ MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC + ">" + "A track");
							}
							producerSupplier.get()
									.send(new ProducerRecord<String, byte[]>(
											MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC,
											SerializationHelper.serialize(track)));
						}

						System.gc();
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

	/**
	 * @param args
	 *            No options supported currently.
	 * @throws URISyntaxException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 */
	public static void main(String[] args) throws URISyntaxException, ParserConfigurationException, SAXException {
		// Load system properties.
		SystemPropertyCenter propertyCenter;
		propertyCenter = new SystemPropertyCenter(args);

		if (propertyCenter.verbose) {
			System.out.println("Starting PedestrianTrackingApp...");
		}

		TopicManager.checkTopics(propertyCenter);

		// Start the pedestrian tracking application.
		PedestrianTrackingApp pedestrianTrackingApp = new PedestrianTrackingApp(propertyCenter);
		pedestrianTrackingApp.initialize(propertyCenter);
		pedestrianTrackingApp.start();
		pedestrianTrackingApp.awaitTermination();
	}
}
