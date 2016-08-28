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

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Arrays;
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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.reid.PedestrianReIDerWithAttr;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.common.TrackWithAttributes;
import org.casia.cripac.isee.vpe.ctrl.TaskData;
import org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.data.MetadataSavingApp;
import org.casia.cripac.isee.vpe.debug.FakePedestrianReIDerWithAttr;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

import scala.Tuple2;

/**
 * The PedestrianReIDApp class is a Spark Streaming application which performs
 * pedestrian re-identification with attributes.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianReIDWithAttrApp extends SparkStreamingApp {

	private static final long serialVersionUID = 6633072492340846871L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "PedestrianReIDWithAttributes";

	/**
	 * Topic to input pedestrian tracks from Kafka.
	 */
	public static final String TRACK_TOPIC = "pedestrian-track-for-pedestrian-reid-with-attr";

	/**
	 * Topic to input pedestrian attributes from Kafka.
	 */
	public static final String ATTR_TOPIC = "pedestrian-attr-pedestrian-for-reid-with-attr";

	/**
	 * Topic to input pedestrian track with attributes from Kafka.
	 */
	public static final String TRACK_WTH_ATTR_TOPIC = "pedestrian-track-with-attr-for-pedestrian-reid-with-attr";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * module, the TopicManager can help register the topics this application
	 * needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(TRACK_TOPIC);
		TopicManager.registerTopic(ATTR_TOPIC);
		TopicManager.registerTopic(TRACK_WTH_ATTR_TOPIC);
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
	 * Topics for inputting attributes. Each assigned a number of threads the
	 * Kafka consumer should use.
	 */
	private Map<String, Integer> attrTopicMap = new HashMap<>();
	/**
	 * Topics for inputting tracks with attributes. Each assigned a number of
	 * threads the Kafka consumer should use. partitions.
	 */
	private Map<String, Integer> trackWithAttrTopicMap = new HashMap<>();
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
	public PedestrianReIDWithAttrApp(SystemPropertyCenter propertyCenter) {
		super();

		trackTopicMap.put(TRACK_TOPIC, propertyCenter.kafkaPartitions);
		attrTopicMap.put(ATTR_TOPIC, propertyCenter.kafkaPartitions);
		trackWithAttrTopicMap.put(TRACK_WTH_ATTR_TOPIC, propertyCenter.kafkaPartitions);

		verbose = propertyCenter.verbose;

		reportListenerAddr = propertyCenter.reportListenerAddress;
		reportListenerPort = propertyCenter.reportListenerPort;

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
		kafkaParams.put("group.id", "PedestrianReIDWithAttrApp" + UUID.randomUUID());
		kafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		// Determine where the stream starts (default: largest)
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		kafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}

	private class AbsoluteTrackID implements Serializable {
		private static final long serialVersionUID = 5528397459807064840L;
		public UUID taskID;
		public int trackID;

		public AbsoluteTrackID(UUID taskID, int trackID) {
			this.taskID = taskID;
			this.trackID = trackID;
		}
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
		final BroadcastSingleton<PedestrianReIDerWithAttr> reIDerSingleton = new BroadcastSingleton<>(
				new ObjectFactory<PedestrianReIDerWithAttr>() {

					private static final long serialVersionUID = -5422299243899032592L;

					@Override
					public PedestrianReIDerWithAttr getObject() {
						return new FakePedestrianReIDerWithAttr();
					}
				}, PedestrianAttrRecognizer.class);
		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(reportListenerAddr, reportListenerPort), SynthesizedLogger.class);

		// Read track bytes in parallel from Kafka.
		JavaPairDStream<String, byte[]> trackBytesStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, trackTopicMap);
		// Recover track from the bytes and extract the ID of the track.
		JavaPairDStream<AbsoluteTrackID, TaskData> trackStream = trackBytesStream
				.mapToPair(new PairFunction<Tuple2<String, byte[]>, AbsoluteTrackID, TaskData>() {

					private static final long serialVersionUID = -485043285469343426L;

					@Override
					public Tuple2<AbsoluteTrackID, TaskData> call(Tuple2<String, byte[]> taskDataBytes)
							throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes._2());
						if (verbose) {
							System.out.println("|INFO|Received " + taskDataBytes._1() + ": " + taskData);
						}
						return new Tuple2<AbsoluteTrackID, TaskData>(
								new AbsoluteTrackID(UUID.fromString(taskDataBytes._1()),
										((Track) taskData.predecessorResult).id),
								taskData);
					}
				});

		// Read attribute bytes in parallel from Kafka.
		JavaPairDStream<String, byte[]> attrBytesStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, attrTopicMap);
		// Recover attributes from the bytes and extract the ID of the track the
		// attributes belong to.
		JavaPairDStream<AbsoluteTrackID, TaskData> attrStream = attrBytesStream
				.mapToPair(new PairFunction<Tuple2<String, byte[]>, AbsoluteTrackID, TaskData>() {

					private static final long serialVersionUID = -485043285469343426L;

					@Override
					public Tuple2<AbsoluteTrackID, TaskData> call(Tuple2<String, byte[]> taskDataBytes)
							throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes._2());

						if (!(taskData.predecessorResult instanceof Attributes)) {
							throw new ClassCastException(
									"Predecessor result is expected to be attributes, but received \""
											+ taskData.predecessorResult + "\"!");
						}

						if (verbose) {
							System.out.println("|INFO|Received " + taskDataBytes._1() + ": " + taskData);
						}
						return new Tuple2<AbsoluteTrackID, TaskData>(
								new AbsoluteTrackID(UUID.fromString(taskDataBytes._1()),
										((Attributes) taskData.predecessorResult).trackID),
								taskData);
					}
				});

		JavaPairDStream<UUID, TaskData> trackWithAttrAssembledStream = trackStream.window(new Duration(30000))
				.join(attrStream.window(new Duration(30000)))
				.mapToPair(new PairFunction<Tuple2<AbsoluteTrackID, Tuple2<TaskData, TaskData>>, UUID, TaskData>() {

					private static final long serialVersionUID = -4988916606490559467L;

					@Override
					public Tuple2<UUID, TaskData> call(Tuple2<AbsoluteTrackID, Tuple2<TaskData, TaskData>> pack)
							throws Exception {
						UUID taskID = pack._1().taskID;
						TaskData taskDataWithTrack = pack._2()._1();
						TaskData taskDataWithAttr = pack._2()._2();
						ExecutionPlan asmPlan = taskDataWithTrack.executionPlan.combine(taskDataWithAttr.executionPlan);

						TaskData asmTaskData = new TaskData(taskDataWithTrack.currentNodeID, asmPlan,
								new TrackWithAttributes((Track) taskDataWithTrack.predecessorResult,
										(Attributes) taskDataWithAttr.predecessorResult));
						if (verbose) {
							System.out.println("|INFO|Assembled track and attr of " + taskID + "-" + pack._1().trackID);
						}
						return new Tuple2<UUID, TaskData>(taskID, asmTaskData);
					}
				});

		// Read track with attribute bytes in parallel from Kafka.
		JavaPairDStream<String, byte[]> trackWithAttrBytesStream = buildBytesDirectInputStream(streamingContext,
				numRecvStreams, kafkaParams, trackWithAttrTopicMap);
		// Recover attributes from the bytes and extract the ID of the track the
		// attributes belong to.
		JavaPairDStream<UUID, TaskData> trackWithAttrIntegralStream = trackWithAttrBytesStream
				.mapToPair(new PairFunction<Tuple2<String, byte[]>, UUID, TaskData>() {

					private static final long serialVersionUID = -25322493045526909L;

					@Override
					public Tuple2<UUID, TaskData> call(Tuple2<String, byte[]> pack) throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(pack._2());
						if (verbose) {
							System.out.println("|INFO|Received " + pack._1() + ": " + taskData);
						}
						return new Tuple2<UUID, TaskData>(UUID.fromString(pack._1()), taskData);
					}
				});

		streamingContext.union(trackWithAttrIntegralStream, Arrays.asList(trackWithAttrAssembledStream))
				.foreachRDD(new VoidFunction<JavaPairRDD<UUID, TaskData>>() {

					private static final long serialVersionUID = 8679793229722440129L;

					@Override
					public void call(JavaPairRDD<UUID, TaskData> trackWithAttrRDD) throws Exception {

						final ObjectSupplier<PedestrianReIDerWithAttr> reIDerSupplier = reIDerSingleton
								.getSupplier(new JavaSparkContext(trackWithAttrRDD.context()));
						final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = broadcastKafkaSink
								.getSupplier(new JavaSparkContext(trackWithAttrRDD.context()));
						final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
								.getSupplier(new JavaSparkContext(trackWithAttrRDD.context()));

						trackWithAttrRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
						trackWithAttrRDD.foreach(new VoidFunction<Tuple2<UUID, TaskData>>() {

							private static final long serialVersionUID = 1663053917263397146L;

							@Override
							public void call(Tuple2<UUID, TaskData> taskWithTrackAndAttr) throws Exception {
								String taskID = taskWithTrackAndAttr._1().toString();
								TaskData taskData = taskWithTrackAndAttr._2();
								TrackWithAttributes trackWithAttr = (TrackWithAttributes) taskData.predecessorResult;

								// Perform ReID.
								int pedestrianID = reIDerSupplier.get().reid(trackWithAttr.track, trackWithAttr.attr);

								// Prepare new task data with the pedestrian ID.
								taskData.predecessorResult = pedestrianID;
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
											.send(new ProducerRecord<String, byte[]>(topic, taskID,
													SerializationHelper.serialize(taskData)));
									RecordMetadata metadata = future.get();
									if (verbose) {
										loggerSupplier.get()
												.info(APP_NAME + ": Sent to Kafka <" + metadata.topic() + "-"
														+ metadata.partition() + "-" + metadata.offset() + "> :"
														+ taskID + ": Pedestrian ID." + pedestrianID);
									}
								}

								// Always send to the meta data saving
								// application.
								Future<RecordMetadata> future = producerSupplier.get()
										.send(new ProducerRecord<String, byte[]>(MetadataSavingApp.PEDESTRIAN_ID_TOPIC,
												taskID, SerializationHelper.serialize(pedestrianID)));
								RecordMetadata metadata = future.get();
								if (verbose) {
									loggerSupplier.get()
											.info(APP_NAME + ": Sent to Kafka: <" + metadata.topic() + "-"
													+ metadata.partition() + "-" + metadata.offset() + ">" + taskID
													+ ": Pedestrian ID." + pedestrianID);
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

		TopicManager.checkTopics(propertyCenter);

		// Start the pedestrian tracking application.
		PedestrianReIDWithAttrApp pedestrianAttrRecogApp = new PedestrianReIDWithAttrApp(propertyCenter);
		pedestrianAttrRecogApp.initialize(propertyCenter);
		pedestrianAttrRecogApp.start();
		pedestrianAttrRecogApp.awaitTermination();
	}
}
