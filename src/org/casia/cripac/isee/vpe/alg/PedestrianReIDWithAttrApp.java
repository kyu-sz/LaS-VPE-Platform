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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
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

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The PedestrianReIDApp class is a Spark Streaming application which performs
 * pedestrian re-identification with attribute.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianReIDWithAttrApp extends SparkStreamingApp {

	private static final long serialVersionUID = 6633072492340846871L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "PedestrianReIDWithAttr";

	/**
	 * Topic to input tracks from Kafka.
	 */
	public static final String TRACK_TOPIC = "track-for-reid-with-attr";

	/**
	 * Topic to input pedestrian attributes from Kafka.
	 */
	public static final String ATTR_TOPIC = "attr-for-reid-with-attr";

	/**
	 * Topic to input pedestrian track with attributes from Kafka.
	 */
	public static final String TRACK_WTH_ATTR_TOPIC = "track-with-attr-for-reid-with-attr";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * whole system, the TopicManager can help register the topics this
	 * application needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(TRACK_TOPIC);
		TopicManager.registerTopic(ATTR_TOPIC);
		TopicManager.registerTopic(TRACK_WTH_ATTR_TOPIC);
	}

	private Properties producerProperties = null;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private boolean verbose = false;
	private Map<String, Integer> trackTopicPartitions = new HashMap<>();
	private Map<String, Integer> attrTopicPartitions = new HashMap<>();
	private Map<String, Integer> trackWithAttrTopicPartitions = new HashMap<>();
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
	public PedestrianReIDWithAttrApp(SystemPropertyCenter propertyCenter) {
		super();

		trackTopicPartitions.put(TRACK_TOPIC, propertyCenter.kafkaPartitions);
		attrTopicPartitions.put(ATTR_TOPIC, propertyCenter.kafkaPartitions);
		trackWithAttrTopicPartitions.put(TRACK_WTH_ATTR_TOPIC, propertyCenter.kafkaPartitions);

		verbose = propertyCenter.verbose;

		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;
		if (verbose) {
			System.out.println(
					"|INFO|Will start " + numRecvStreams + " streams concurrently to receive messgaes from Kafka.");
		}

		producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		producerProperties.put("producer.type", "sync");
		producerProperties.put("request.required.acks", "1");
		producerProperties.put("compression.codec", "gzip");
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
				new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort), SynthesizedLogger.class);

		// Read track bytes in parallel from Kafka.
		List<JavaPairDStream<String, byte[]>> parTrackBytesStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parTrackBytesStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class,
					StringDecoder.class, DefaultDecoder.class, commonKafkaParams, trackTopicPartitions,
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		// Union the parallel track bytes streams.
		JavaPairDStream<String, byte[]> trackBytesStream = streamingContext.union(parTrackBytesStreams.get(0),
				parTrackBytesStreams.subList(1, parTrackBytesStreams.size()));
		// Recover track from the bytes and extract the ID of the track.
		JavaPairDStream<Tuple2<String, Integer>, TaskData> trackStream = trackBytesStream
				.mapToPair(new PairFunction<Tuple2<String, byte[]>, Tuple2<String, Integer>, TaskData>() {

					private static final long serialVersionUID = -485043285469343426L;

					@Override
					public Tuple2<Tuple2<String, Integer>, TaskData> call(Tuple2<String, byte[]> taskDataBytes)
							throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes._2());
						return new Tuple2<Tuple2<String, Integer>, TaskData>(new Tuple2<String, Integer>(
								taskDataBytes._1(), ((Track) taskData.predecessorResult).id), taskData);
					}
				});

		// Read attribute bytes in parallel from Kafka.
		List<JavaPairDStream<String, byte[]>> parAttrBytesStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parAttrBytesStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class,
					StringDecoder.class, DefaultDecoder.class, commonKafkaParams, attrTopicPartitions,
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		// Union the parallel attribute bytes streams.
		JavaPairDStream<String, byte[]> attrBytesStream = streamingContext.union(parAttrBytesStreams.get(0),
				parAttrBytesStreams.subList(1, parAttrBytesStreams.size()));
		// Recover attributes from the bytes and extract the ID of the track the
		// attributes belong to.
		JavaPairDStream<Tuple2<String, Integer>, TaskData> attrStream = attrBytesStream
				.mapToPair(new PairFunction<Tuple2<String, byte[]>, Tuple2<String, Integer>, TaskData>() {

					private static final long serialVersionUID = -485043285469343426L;

					@Override
					public Tuple2<Tuple2<String, Integer>, TaskData> call(Tuple2<String, byte[]> taskDataBytes)
							throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes._2());
						return new Tuple2<Tuple2<String, Integer>, TaskData>(new Tuple2<String, Integer>(
								taskDataBytes._1(), ((Attributes) taskData.predecessorResult).trackID), taskData);
					}
				});

		JavaPairDStream<String, TaskData> trackWithAttrAssembledStream = trackStream.join(attrStream).mapToPair(
				new PairFunction<Tuple2<Tuple2<String, Integer>, Tuple2<TaskData, TaskData>>, String, TaskData>() {

					private static final long serialVersionUID = -4988916606490559467L;

					@Override
					public Tuple2<String, TaskData> call(
							Tuple2<Tuple2<String, Integer>, Tuple2<TaskData, TaskData>> pack) throws Exception {
						String taskID = pack._1()._1();
						TaskData track = pack._2()._1();
						TaskData attr = pack._2()._2();
						ExecutionPlan combinedPlan = track.executionPlan.combine(attr.executionPlan);
						TaskData combinedTaskData = new TaskData(track.currentNodeID, combinedPlan,
								new TrackWithAttributes((Track) track.predecessorResult,
										(Attributes) attr.predecessorResult));
						return new Tuple2<String, TaskData>(taskID, combinedTaskData);
					}
				});

		// Read track with attribute bytes in parallel from Kafka.
		List<JavaPairDStream<String, byte[]>> parTrackWithAttrBytesStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parAttrBytesStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class,
					StringDecoder.class, DefaultDecoder.class, commonKafkaParams, attrTopicPartitions,
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		// Union the parallel attribute bytes streams.
		JavaPairDStream<String, byte[]> trackWithAttrBytesStream = streamingContext.union(
				parTrackWithAttrBytesStreams.get(0),
				parTrackWithAttrBytesStreams.subList(1, parTrackWithAttrBytesStreams.size()));
		// Recover attributes from the bytes and extract the ID of the track the
		// attributes belong to.
		JavaPairDStream<String, TaskData> trackWithAttrIntegralStream = trackWithAttrBytesStream
				.mapValues(new Function<byte[], TaskData>() {

					private static final long serialVersionUID = 3372014000725297171L;

					@Override
					public TaskData call(byte[] taskDataBytes) throws Exception {
						return (TaskData) SerializationHelper.deserialize(taskDataBytes);
					}
				});

		streamingContext.union(trackWithAttrIntegralStream, Arrays.asList(trackWithAttrAssembledStream))
				.foreachRDD(new VoidFunction<JavaPairRDD<String, TaskData>>() {

					private static final long serialVersionUID = 8679793229722440129L;

					@Override
					public void call(JavaPairRDD<String, TaskData> trackWithAttrRDD) throws Exception {

						final ObjectSupplier<PedestrianReIDerWithAttr> reIDerSupplier = reIDerSingleton
								.getSupplier(new JavaSparkContext(trackWithAttrRDD.context()));
						final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = broadcastKafkaSink
								.getSupplier(new JavaSparkContext(trackWithAttrRDD.context()));
						final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
								.getSupplier(new JavaSparkContext(trackWithAttrRDD.context()));

						trackWithAttrRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
						trackWithAttrRDD.foreach(new VoidFunction<Tuple2<String, TaskData>>() {

							private static final long serialVersionUID = 1663053917263397146L;

							@Override
							public void call(Tuple2<String, TaskData> taskWithTrackAndAttr) throws Exception {
								String taskID = taskWithTrackAndAttr._1();
								TaskData taskData = taskWithTrackAndAttr._2();
								TrackWithAttributes trackWithAttr = (TrackWithAttributes) taskData.predecessorResult;

								// Perform ReID.
								int pedestrianID = reIDerSupplier.get().reid(trackWithAttr.track, trackWithAttr.attr);

								// Prepare new task data.
								// Stored the track in the task data, which can
								// be
								// cyclic utilized.
								taskData.predecessorResult = pedestrianID;
								// Get the IDs of successor nodes.
								Set<Integer> successorIDs = taskData.executionPlan
										.getSuccessors(taskData.currentNodeID);
								// Mark the current node as executed.
								taskData.executionPlan.markExecuted(taskData.currentNodeID);
								// Send to all the successor nodes.
								for (int successorID : successorIDs) {
									taskData.currentNodeID = successorID;
									String topic = taskData.executionPlan.getInputTopicName(successorID);

									if (verbose) {
										loggerSupplier.get().info("PedestrianTrackingApp: Sending to Kafka <" + topic
												+ "> :" + "An attribute");
									}
									producerSupplier.get().send(new ProducerRecord<String, byte[]>(topic, taskID,
											SerializationHelper.serialize(taskData)));
								}

								// Always send to the meta data saving
								// application.
								if (verbose) {
									loggerSupplier.get()
											.info("PedestrianAttrRecogApp: Sending to Kafka: <"
													+ MetadataSavingApp.PEDESTRIAN_ID_TOPIC + ">"
													+ "An attribute");
								}
								producerSupplier.get()
										.send(new ProducerRecord<String, byte[]>(
												MetadataSavingApp.PEDESTRIAN_ID_TOPIC,
												SerializationHelper.serialize(pedestrianID)));

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
