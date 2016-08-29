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
package org.casia.cripac.isee.vpe.data;

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
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.casia.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.TaskData;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

import scala.Tuple2;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class DataFeedingApp extends SparkStreamingApp {

	private static final long serialVersionUID = 2653710161488980937L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "DataFeeding";
	public static final String PEDESTRIAN_TRACK_RTRV_JOB_TOPIC = "pedestrian-track-rtrv-job";
	public static final String PEDESTRIAN_TRACK_WITH_ATTR_RTRV_JOB_TOPIC = "pedestrian-track-with-attr-rtrv-job";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * module, the TopicManager can help register the topics this application
	 * needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(PEDESTRIAN_TRACK_RTRV_JOB_TOPIC);
		TopicManager.registerTopic(PEDESTRIAN_TRACK_WITH_ATTR_RTRV_JOB_TOPIC);
	}

	private Properties producerProperties = null;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private boolean verbose = false;
	private Map<String, Integer> trackRtrvJobTopicMap = new HashMap<>();
	private Map<String, Integer> trackWithAttrRtrvJobTopicMap = new HashMap<>();
	private String messageListenerAddr;
	private int messageListenerPort;
	private int numRecvStreams;

	/**
	 * Constructor of the application, configuring properties read from a
	 * property center.
	 * 
	 * @param propertyCenter
	 *            A class saving all the properties this application may need.
	 */
	public DataFeedingApp(SystemPropertyCenter propertyCenter) {
		super();

		trackRtrvJobTopicMap.put(PEDESTRIAN_TRACK_RTRV_JOB_TOPIC, propertyCenter.kafkaPartitions);
		trackWithAttrRtrvJobTopicMap.put(PEDESTRIAN_TRACK_WITH_ATTR_RTRV_JOB_TOPIC, propertyCenter.kafkaPartitions);

		verbose = propertyCenter.verbose;

		messageListenerAddr = propertyCenter.reportListenerAddress;
		messageListenerPort = propertyCenter.reportListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;
		if (verbose) {
			System.out.println(
					"|INFO|Will start " + numRecvStreams + " streams concurrently to receive messgaes from Kafka.");
		}

		producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		producerProperties.put("compression.codec", "1");
		producerProperties.put("max.request.size", "10000000");
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

		// Common kafka settings.
		commonKafkaParams.put("group.id", "DataFeedingApp" + UUID.randomUUID());
		commonKafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		// Determine where the stream starts (default: largest)
		commonKafkaParams.put("auto.offset.reset", "smallest");
		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		commonKafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.vpe.common.SparkStreamingApp#getStreamContext()
	 */
	@Override
	protected JavaStreamingContext getStreamContext() {
		FakeDatabaseConnector databaseConnector = new FakeDatabaseConnector();

		// Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		// Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> broadcastKafkaSink = new BroadcastSingleton<>(
				new KafkaProducerFactory<String, byte[]>(producerProperties), KafkaProducer.class);
		// Create ResourceSink for any other unserializable components.
		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort), SynthesizedLogger.class);

		// Read track retrieving jobs in parallel from Kafka.
		buildBytesDirectInputStream(streamingContext, numRecvStreams, commonKafkaParams, trackRtrvJobTopicMap)
				// Retrieve and deliever tracks.
				.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

					private static final long serialVersionUID = 2398785978507302303L;

					@Override
					public void call(JavaPairRDD<String, byte[]> jobRDD) throws Exception {

						final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = broadcastKafkaSink
								.getSupplier(new JavaSparkContext(jobRDD.context()));
						final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
								.getSupplier(new JavaSparkContext(jobRDD.context()));

						jobRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
						jobRDD.foreach(new VoidFunction<Tuple2<String, byte[]>>() {

							private static final long serialVersionUID = -3787928455732734520L;

							@Override
							public void call(Tuple2<String, byte[]> job) throws Exception {
								TaskData taskData = (TaskData) SerializationHelper.deserialize(job._2());
								String jobParam = (String) taskData.predecessorResult;
								String[] paramParts = jobParam.split(":");
								Track track = databaseConnector.getTrack(paramParts[0], paramParts[1]);
								taskData.predecessorResult = track;

								// Get the IDs of successor nodes.
								int[] successorIDs = taskData.executionPlan.getNode(taskData.currentNodeID)
										.getSuccessors();
								// Mark the current node as executed.
								taskData.executionPlan.markExecuted(taskData.currentNodeID);
								// Send to all the successor nodes.
								for (int successorID : successorIDs) {
									taskData.currentNodeID = successorID;
									String topic = taskData.executionPlan.getNode(successorID).getTopic();

									Future<RecordMetadata> future = producerSupplier.get()
											.send(new ProducerRecord<String, byte[]>(topic, job._1(),
													SerializationHelper.serialize(taskData)));
									RecordMetadata metadata = future.get();
									if (verbose) {
										loggerSupplier.get()
												.info(APP_NAME + ": Sent to Kafka <" + metadata.topic() + "-"
														+ metadata.partition() + "-" + metadata.offset() + ">: "
														+ job._1() + ": " + taskData);
									}
								}
							}
						});
					}
				});

		// Read track with attributes retrieving jobs in parallel from Kafka.
		buildBytesDirectInputStream(streamingContext, numRecvStreams, commonKafkaParams, trackWithAttrRtrvJobTopicMap)
				// Retrieve and deliever tracks with attributes.
				.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

					private static final long serialVersionUID = 2398785978507302303L;

					@Override
					public void call(JavaPairRDD<String, byte[]> jobRDD) throws Exception {

						final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = broadcastKafkaSink
								.getSupplier(new JavaSparkContext(jobRDD.context()));
						final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
								.getSupplier(new JavaSparkContext(jobRDD.context()));

						jobRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
						jobRDD.foreach(new VoidFunction<Tuple2<String, byte[]>>() {

							private static final long serialVersionUID = -3787928455732734520L;

							@Override
							public void call(Tuple2<String, byte[]> job) throws Exception {
								TaskData taskData = (TaskData) SerializationHelper.deserialize(job._2());
								String jobParam = (String) taskData.predecessorResult;
								String[] paramParts = jobParam.split(":");
								PedestrianInfo trackWithAttr = databaseConnector.getTrackWithAttr(paramParts[0],
										paramParts[1]);
								taskData.predecessorResult = trackWithAttr;

								// Get the IDs of successor nodes.
								int[] successorIDs = taskData.executionPlan.getNode(taskData.currentNodeID)
										.getSuccessors();
								// Mark the current node as executed.
								taskData.executionPlan.markExecuted(taskData.currentNodeID);
								// Send to all the successor nodes.
								for (int successorID : successorIDs) {
									taskData.currentNodeID = successorID;
									String topic = taskData.executionPlan.getNode(successorID).getTopic();

									Future<RecordMetadata> future = producerSupplier.get()
											.send(new ProducerRecord<String, byte[]>(topic, job._1(),
													SerializationHelper.serialize(taskData)));
									RecordMetadata metadata = future.get();
									if (verbose) {
										loggerSupplier.get()
												.info(APP_NAME + ": Sent to Kafka <" + metadata.topic() + "-"
														+ metadata.partition() + "-" + metadata.offset() + "> :"
														+ job._1() + ": " + taskData);
									}
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

		TopicManager.checkTopics(propertyCenter);

		// Start the pedestrian tracking application.
		DataFeedingApp dataFeedingApp = new DataFeedingApp(propertyCenter);
		dataFeedingApp.initialize(propertyCenter);
		dataFeedingApp.start();
		dataFeedingApp.awaitTermination();
	}
}
