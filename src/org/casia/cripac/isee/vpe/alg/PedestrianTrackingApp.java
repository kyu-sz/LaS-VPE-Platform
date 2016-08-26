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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.TaskData;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.data.MetadataSavingApp;
import org.casia.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

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

	/**
	 * Topic to input video URLs from Kafka.
	 */
	public static final String VIDEO_URL_TOPIC = "video-url-for-pedestrian-tracking";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * module, the TopicManager can help register the topics this application
	 * needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(VIDEO_URL_TOPIC);
	}

	/**
	 * Properties of Kafka producer.
	 */
	private Properties producerProperties = new Properties();
	/**
	 * Configuration for Spark.
	 */
	private transient SparkConf sparkConf;
	/**
	 * Kafka parameters for creating input streams pulling messages from Kafka
	 * Brokers.
	 */
	private Map<String, String> kafkaParams = new HashMap<>();
	/**
	 * Whether to output verbose informations.
	 */
	private boolean verbose = false;
	/**
	 * Topics for inputting video URLs. Each assigned a number of threads the
	 * Kafka consumer should use.
	 */
	private Map<String, Integer> videoURLTopicMap = new HashMap<>();
	/**
	 * The address listening to reports.
	 */
	private String reportListenerAddr;
	/**
	 * The port of the address listening to reports.
	 */
	private int reportListenerPort;
	/**
	 * Number of parallel streams pulling messages from Kafka Brokers.
	 */
	private int numRecvStreams;

	/**
	 * Constructor of the application, configuring properties read from a
	 * property center.
	 * 
	 * @param propertyCenter
	 *            A class saving all the properties this application may need.
	 */
	public PedestrianTrackingApp(SystemPropertyCenter propertyCenter) {
		super();

		verbose = propertyCenter.verbose;

		reportListenerAddr = propertyCenter.reportListenerAddress;
		reportListenerPort = propertyCenter.reportListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		// taskTopicsSet.add(PEDESTRIAN_TRACKING_TASK_TOPIC);
		videoURLTopicMap.put(VIDEO_URL_TOPIC, propertyCenter.kafkaPartitions);

		producerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		// Create contexts.
		sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");

		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf.setMaster(propertyCenter.sparkMaster).set("deploy.mode",
					propertyCenter.sparkDeployMode);
		}

		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		kafkaParams.put("group.id", "PedestrianTrackingApp" + UUID.randomUUID());
		kafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		// Determine where the stream starts (default: largest)
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.vpe.common.SparkStreamingApp#getStreamContext()
	 */
	@Override
	protected JavaStreamingContext getStreamContext() {
		// Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		// Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> producerSingleton = new BroadcastSingleton<KafkaProducer<String, byte[]>>(
				new KafkaProducerFactory<>(producerProperties), KafkaProducer.class);
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
				new SynthesizedLoggerFactory(reportListenerAddr, reportListenerPort), SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 */
		JavaPairDStream<String, byte[]> jobStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, videoURLTopicMap);
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
						TaskData taskData = (TaskData) SerializationHelper.deserialize(task._2());
						// Get the URL of the video to process from the
						// execution data of this node.
						String videoURL = (String) taskData.predecessorResult;
						// Get the IDs of successor nodes.
						int[] successorIDs = taskData.executionPlan.getNode(taskData.currentNodeID).getSuccessors();
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
								String topic = taskData.executionPlan.getNode(successorID).getTopic();

								Future<RecordMetadata> future = producerSupplier.get()
										.send(new ProducerRecord<String, byte[]>(topic, task._1(),
												SerializationHelper.serialize(taskData)));
								RecordMetadata metadata = future.get();
								if (verbose) {
									loggerSupplier.get()
											.info(APP_NAME + ": Sent to Kafka <" + metadata.topic() + "-"
													+ metadata.partition() + "-" + metadata.offset() + ">: "
													+ "Track of " + task._1() + "-" + track.id);
								}
							}

							// Always send to the meta data saving application.
							Future<RecordMetadata> future = producerSupplier.get()
									.send(new ProducerRecord<String, byte[]>(MetadataSavingApp.PEDESTRIAN_TRACK_TOPIC,
											SerializationHelper.serialize(track)));
							RecordMetadata metadata = future.get();
							if (verbose) {
								loggerSupplier.get().info(APP_NAME + ": Sent to Kafka <" + metadata.topic() + "-"
										+ metadata.partition() + "-" + metadata.offset() + ">: " + "Track");
							}
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
