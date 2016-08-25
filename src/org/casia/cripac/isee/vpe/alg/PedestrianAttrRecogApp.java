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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
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
import org.casia.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

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
	 * Topic to input tracks from Kafka.
	 */
	public static final String TRACK_TOPIC = "pedestrian-track-for-pedestrian-attr-recog";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * module, the TopicManager can help register the topics this application
	 * needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(TRACK_TOPIC);
	}

	/**
	 * Properties of Kafka producer.
	 */
	private Properties producerProperties = null;
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
	 * Topics for inputting tracks. Each assigned a number of threads the Kafka
	 * consumer should use.
	 */
	private Map<String, Integer> trackTopicMap = new HashMap<>();
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
	public PedestrianAttrRecogApp(SystemPropertyCenter propertyCenter) {
		super();

		trackTopicMap.put(TRACK_TOPIC, propertyCenter.kafkaPartitions);

		verbose = propertyCenter.verbose;

		reportListenerAddr = propertyCenter.reportListenerAddress;
		reportListenerPort = propertyCenter.reportListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		producerProperties = new Properties();
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

		// Common kafka settings.
		kafkaParams.put("group.id", "PedestrianAttrRecogApp" + UUID.randomUUID());
		kafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		// Determine where the stream starts (default: largest)
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
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
		final BroadcastSingleton<KafkaProducer<String, byte[]>> broadcastKafkaSink = new BroadcastSingleton<>(
				new KafkaProducerFactory<String, byte[]>(producerProperties), KafkaProducer.class);
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
				new SynthesizedLoggerFactory(reportListenerAddr, reportListenerPort), SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 */
		JavaPairDStream<String, byte[]> trackBytesStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, trackTopicMap);
		// //Retrieve data from Kafka.
		// JavaPairInputDStream<String, byte[]> trackBytesDStream =
		// KafkaUtils.createDirectStream(streamingContext, String.class,
		// byte[].class,
		// StringDecoder.class, DefaultDecoder.class, commonKafkaParams,
		// inputTopicsSet);

		// Extract tracks from the data.
		JavaPairDStream<String, TaskData> trackStream = trackBytesStream.mapValues(new Function<byte[], TaskData>() {

			private static final long serialVersionUID = -2138675698164723884L;

			@Override
			public TaskData call(byte[] taskDataBytes) throws Exception {
				return (TaskData) SerializationHelper.deserialize(taskDataBytes);
			}
		});

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
								loggerSupplier.get().info("PedestrianAttrRecogApp: Sending to Kafka <" + topic + ">: "
										+ "Attributes of " + taskID + "-" + track.id);
							}
							producerSupplier.get().send(new ProducerRecord<String, byte[]>(topic, taskID,
									SerializationHelper.serialize(taskData)));
						}

						// Always send to the meta data saving application.
						if (verbose) {
							loggerSupplier.get().info("PedestrianAttrRecogApp: Sending to Kafka <"
									+ MetadataSavingApp.PEDESTRIAN_ATTR_TOPIC + ">: " + "Attributes");
						}
						producerSupplier.get().send(new ProducerRecord<String, byte[]>(
								MetadataSavingApp.PEDESTRIAN_ATTR_TOPIC, SerializationHelper.serialize(attributes)));

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
