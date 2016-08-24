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
import java.util.Arrays;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.ctrl.TaskData;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.casia.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The PedestrianAttrRecogApp class is a Spark Streaming application which
 * performs pedestrian attribute recognition.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianAttrRecogApp extends SparkStreamingApp {

	private static final long serialVersionUID = 3104859533881615664L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "PedestrianAttributeRecognizing";
	
	/**
	 * Topic to input attribute recognition tasks. The tasks would tell the
	 * application to fetch input data from database or hard disks.
	 */
	public static final String PEDESTRIAN_ATTR_RECOG_JOB_TOPIC = "pedestrian-attr-recog-job";
	
	/**
	 * Topic to input tracks from Kafka. The application directly performs
	 * recognition on these tracks, rather than fetching input data from other
	 * sources.
	 */
	public static final String PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC = "pedestrian-attr-recog-track-input";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * whole system, the TopicManager can help register the topics this
	 * application needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(PEDESTRIAN_ATTR_RECOG_JOB_TOPIC);
		TopicManager.registerTopic(PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC);
	}

	private Properties attrProducerProperties = null;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private boolean verbose = false;
	// private HashSet<String> inputTopicsSet = new HashSet<>();
	// private HashSet<String> taskTopicsSet = new HashSet<>();
	private Map<String, Integer> trackTopicPartitions = new HashMap<>();
	private Map<String, Integer> jobTopicPartitions = new HashMap<>();
	private String messageListenerAddr;
	private int messageListenerPort;
	private int numRecvStreams;

	/**
	 * Constructor of the application, configuring properties read from a property center.
	 * @param propertyCenter	A class saving all the properties this application may need.
	 */
	public PedestrianAttrRecogApp(SystemPropertyCenter propertyCenter) {
		super();

		// taskTopicsSet.add(PEDESTRIAN_ATTR_RECOG_TASK_TOPIC);
		// inputTopicsSet.add(PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);
		jobTopicPartitions.put(PEDESTRIAN_ATTR_RECOG_JOB_TOPIC, propertyCenter.kafkaPartitions);
		trackTopicPartitions.put(PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC, propertyCenter.kafkaPartitions);

		verbose = propertyCenter.verbose;

		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		attrProducerProperties = new Properties();
		attrProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		attrProducerProperties.put("producer.type", "sync");
		attrProducerProperties.put("request.required.acks", "1");
		attrProducerProperties.put("compression.codec", "gzip");
		attrProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		attrProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

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
		commonKafkaParams.put("group.id", "PedestrianAttrRecogApp" + UUID.randomUUID());
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
		// Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		// Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> broadcastKafkaSink = new BroadcastSingleton<>(
				new KafkaProducerFactory<String, byte[]>(attrProducerProperties), KafkaProducer.class);
		// Create ResourceSink for any other unserializable components.
		final BroadcastSingleton<PedestrianAttrRecognizer> attrRecognizerSingleton = new BroadcastSingleton<>(
				new ObjectFactory<PedestrianAttrRecognizer>() {

					private static final long serialVersionUID = -5422299243899032592L;

					@Override
					public PedestrianAttrRecognizer getObject() {
						return new FakePedestrianAttrRecognizer();
					}
				}, PedestrianAttrRecognizer.class);
		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort), SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 */
		List<JavaPairDStream<String, byte[]>> parTrackBytesStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parTrackBytesStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class,
					StringDecoder.class, DefaultDecoder.class, commonKafkaParams, trackTopicPartitions,
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		JavaPairDStream<String, byte[]> trackBytesStream = streamingContext.union(parTrackBytesStreams.get(0),
				parTrackBytesStreams.subList(1, parTrackBytesStreams.size()));
		// //Retrieve data from Kafka.
		// JavaPairInputDStream<String, byte[]> trackBytesDStream =
		// KafkaUtils.createDirectStream(streamingContext, String.class,
		// byte[].class,
		// StringDecoder.class, DefaultDecoder.class, commonKafkaParams,
		// inputTopicsSet);

		// Extract tracks from the data.
		JavaPairDStream<String, TaskData> trackStreamFromKafka = trackBytesStream
				.mapValues(new Function<byte[], TaskData>() {

					private static final long serialVersionUID = -2138675698164723884L;

					@Override
					public TaskData call(byte[] taskDataBytes) throws Exception {
						return (TaskData) SerializationHelper.deserialize(taskDataBytes);
					}
				});

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 */
		List<JavaPairDStream<String, byte[]>> parJobStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parJobStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class, StringDecoder.class,
					DefaultDecoder.class, commonKafkaParams, jobTopicPartitions, StorageLevel.MEMORY_AND_DISK_SER()));
		}
		JavaPairDStream<String, byte[]> jobStream = streamingContext.union(parJobStreams.get(0),
				parJobStreams.subList(1, parJobStreams.size()));
		// //Get tasks from Kafka.
		// JavaPairInputDStream<String, byte[]> taskDStream =
		// KafkaUtils.createDirectStream(streamingContext, String.class,
		// byte[].class,
		// StringDecoder.class, DefaultDecoder.class, commonKafkaParams,
		// taskTopicsSet);

		// Retrieve tracks from database.
		FakeDatabaseConnector databaseConnector = new FakeDatabaseConnector();
		JavaPairDStream<String, TaskData> trackStreamFromJob = jobStream.mapValues(new Function<byte[], TaskData>() {

			private static final long serialVersionUID = 2423448095433148528L;

			@Override
			public TaskData call(byte[] taskDataBytes) throws Exception {
				TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes);
				String jobParam = (String) taskData.predecessorResult;
				String[] paramParts = jobParam.split(":");
				Track track = databaseConnector.getTrack(paramParts[0], paramParts[1]);
				taskData.predecessorResult = track;
				return taskData;
			}
		});

		JavaPairDStream<String, TaskData> trackStream = streamingContext.union(trackStreamFromKafka,
				Arrays.asList(trackStreamFromJob));

		// Recognize attributes from the tracks, then send them to the metadata
		// saving application.
		trackStream.foreachRDD(new VoidFunction<JavaPairRDD<String, TaskData>>() {

			private static final long serialVersionUID = 8679793229722440129L;

			@Override
			public void call(JavaPairRDD<String, TaskData> trackRDD) throws Exception {

				final ObjectSupplier<PedestrianAttrRecognizer> recognizerSupplier = attrRecognizerSingleton
						.getSupplier(new JavaSparkContext(trackRDD.context()));
				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = broadcastKafkaSink
						.getSupplier(new JavaSparkContext(trackRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(trackRDD.context()));

				trackRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				trackRDD.foreach(new VoidFunction<Tuple2<String, TaskData>>() {

					private static final long serialVersionUID = 1663053917263397146L;

					@Override
					public void call(Tuple2<String, TaskData> taskWithTrack) throws Exception {
						String taskID = taskWithTrack._1();
						TaskData taskData = taskWithTrack._2();
						Track track = (Track) taskData.predecessorResult;

						// Recognize attributes.
						Attributes attributes = recognizerSupplier.get().recognize(track);

						// Prepare new task data.
						// Stored the track in the task data, which can be
						// cyclic utilized.
						taskData.predecessorResult = attributes;
						// Get the IDs of successor nodes.
						Set<Integer> successorIDs = taskData.executionPlan.getSuccessors(taskData.currentNodeID);
						// Mark the current node as executed.
						taskData.executionPlan.markExecuted(taskData.currentNodeID);
						// Send to all the successor nodes.
						for (int successorID : successorIDs) {
							taskData.currentNodeID = successorID;
							String topic = taskData.executionPlan.getInputTopicName(successorID);

							if (verbose) {
								loggerSupplier.get().info(
										"PedestrianTrackingApp: Sending to Kafka <" + topic + "> :" + "Attributes");
							}
							producerSupplier.get().send(new ProducerRecord<String, byte[]>(topic, taskID,
									SerializationHelper.serialize(taskData)));
						}

						// Always send to the meta data saving application.
						if (verbose) {
							loggerSupplier.get().info("PedestrianAttrRecogApp: Sending to Kafka: <"
									+ MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC + ">" + "Attributes");
						}
						producerSupplier.get()
								.send(new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC,
										SerializationHelper.serialize(attributes)));

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

		TopicManager.checkTopics(propertyCenter);

		// Start the pedestrian tracking application.
		PedestrianAttrRecogApp pedestrianAttrRecogApp = new PedestrianAttrRecogApp(propertyCenter);
		pedestrianAttrRecogApp.initialize(propertyCenter);
		pedestrianAttrRecogApp.start();
		pedestrianAttrRecogApp.awaitTermination();
	}
}
