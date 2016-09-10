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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.casia.cripac.isee.pedestrian.reid.PedestrianReIDer;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.TaskData;
import org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.data.DataManagingApp;
import org.casia.cripac.isee.vpe.debug.FakePedestrianReIDerWithAttr;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * The PedestrianReIDApp class is a Spark Streaming application which performs
 * pedestrian re-identification with attributes.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianReIDUsingAttrApp extends SparkStreamingApp {

	private static final long serialVersionUID = 6633072492340846871L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "PedestrianReIDUsingAttr";

	/**
	 * Topic to input pedestrian tracks from Kafka.
	 */
	public static final String TRACK_TOPIC = "pedestrian-track-for-pedestrian-reid-using-attr";

	/**
	 * Topic to input pedestrian attributes from Kafka.
	 */
	public static final String ATTR_TOPIC = "pedestrian-attr-pedestrian-for-reid-using-attr";

	/**
	 * Topic to input pedestrian track with attributes from Kafka.
	 */
	public static final String TRACK_WTH_ATTR_TOPIC = "pedestrian-track-with-attr-for-pedestrian-reid-using-attr";

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
	 * Duration for buffering results.
	 */
	private int bufDuration;

	/**
	 * Constructor of the application, configuring properties read from a
	 * property center.
	 * 
	 * @param propertyCenter
	 *            A class saving all the properties this application may need.
	 */
	public PedestrianReIDUsingAttrApp(SystemPropertyCenter propertyCenter) {
		super();

		trackTopicMap.put(TRACK_TOPIC, propertyCenter.kafkaPartitions);
		attrTopicMap.put(ATTR_TOPIC, propertyCenter.kafkaPartitions);
		trackWithAttrTopicMap.put(TRACK_WTH_ATTR_TOPIC, propertyCenter.kafkaPartitions);

		verbose = propertyCenter.verbose;

		reportListenerAddr = propertyCenter.reportListenerAddress;
		reportListenerPort = propertyCenter.reportListenerPort;

		bufDuration = propertyCenter.bufDuration;
		numRecvStreams = propertyCenter.numRecvStreams;

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
		final BroadcastSingleton<PedestrianReIDer> reIDerSingleton = new BroadcastSingleton<>(
				new ObjectFactory<PedestrianReIDer>() {

					private static final long serialVersionUID = -5422299243899032592L;

					@Override
					public PedestrianReIDer getObject() {
						return new FakePedestrianReIDerWithAttr();
					}
				}, PedestrianReIDer.class);
		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(reportListenerAddr, reportListenerPort), SynthesizedLogger.class);

		// Read track bytes in parallel from Kafka.
		// Recover track from the bytes and extract the ID of the track.
		JavaPairDStream<String, TaskData> trackStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, trackTopicMap).mapToPair(new PairFunction<Tuple2<String, byte[]>, String, TaskData>() {

					private static final long serialVersionUID = -485043285469343426L;

					@Override
					public Tuple2<String, TaskData> call(Tuple2<String, byte[]> taskDataBytes) throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes._2());
						return new Tuple2<String, TaskData>(
								taskDataBytes._1() + ":" + ((Track) taskData.predecessorResult).id, taskData);
					}
				});

		// Announce received track.
		trackStream.foreachRDD(new VoidFunction<JavaPairRDD<String, TaskData>>() {

			private static final long serialVersionUID = 802003680530339206L;

			@Override
			public void call(JavaPairRDD<String, TaskData> rdd) throws Exception {
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(rdd.context()));
				rdd.foreach(new VoidFunction<Tuple2<String, TaskData>>() {

					private static final long serialVersionUID = -3529294947680224456L;

					@Override
					public void call(Tuple2<String, TaskData> track) throws Exception {
						if (verbose) {
							loggerSupplier.get().info("Received track: " + track._1());
						}
					}
				});
			}
		});

		// Read attribute bytes in parallel from Kafka.
		// Recover attributes from the bytes and extract the ID of the track the
		// attributes belong to.
		JavaPairDStream<String, TaskData> attrStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, attrTopicMap).mapToPair(new PairFunction<Tuple2<String, byte[]>, String, TaskData>() {

					private static final long serialVersionUID = -485043285469343426L;

					@Override
					public Tuple2<String, TaskData> call(Tuple2<String, byte[]> taskDataBytes) throws Exception {
						TaskData taskData = (TaskData) SerializationHelper.deserialize(taskDataBytes._2());

						if (!(taskData.predecessorResult instanceof Attributes)) {
							throw new ClassCastException(
									"Predecessor result is expected to be attributes, but received \""
											+ taskData.predecessorResult + "\"!");
						}

						if (verbose) {
							System.out.println("|INFO|Received " + taskDataBytes._1() + ": " + taskData);
						}
						return new Tuple2<String, TaskData>(
								taskDataBytes._1() + ":" + ((Attributes) taskData.predecessorResult).trackID, taskData);
					}
				});

		// Announce received attributes.
		attrStream.foreachRDD(new VoidFunction<JavaPairRDD<String, TaskData>>() {

			private static final long serialVersionUID = 802003680510339206L;

			@Override
			public void call(JavaPairRDD<String, TaskData> rdd) throws Exception {
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(rdd.context()));
				rdd.foreach(new VoidFunction<Tuple2<String, TaskData>>() {

					private static final long serialVersionUID = -3529294947680224456L;

					@Override
					public void call(Tuple2<String, TaskData> attr) throws Exception {
						if (verbose) {
							loggerSupplier.get().info("Received attribute: " + attr._1());
						}
					}
				});
			}
		});

		// Join the track stream and attribute stream, tolerating failure.
		JavaPairDStream<String, Tuple2<Optional<TaskData>, Optional<TaskData>>> unsurelyJoinedStream = trackStream
				.fullOuterJoin(attrStream);

		// Filter out instantly joined pairs.
		JavaPairDStream<String, Tuple2<TaskData, TaskData>> instantlyJoinedStream = unsurelyJoinedStream
				.filter(new Function<Tuple2<String, Tuple2<Optional<TaskData>, Optional<TaskData>>>, Boolean>() {

					private static final long serialVersionUID = -2343091659648238232L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<Optional<TaskData>, Optional<TaskData>>> item)
							throws Exception {
						return item._2()._1().isPresent() && item._2()._2().isPresent();
					}
				})
				.mapValues(new Function<Tuple2<Optional<TaskData>, Optional<TaskData>>, Tuple2<TaskData, TaskData>>() {

					private static final long serialVersionUID = 2610836030144683294L;

					@Override
					public Tuple2<TaskData, TaskData> call(Tuple2<Optional<TaskData>, Optional<TaskData>> optPair)
							throws Exception {
						return new Tuple2<TaskData, TaskData>(optPair._1().get(), optPair._2().get());
					}
				});

		// Filter out tracks that cannot find attributes to match.
		JavaPairDStream<String, TaskData> unjoinedTrackStream = unsurelyJoinedStream
				.filter(new Function<Tuple2<String, Tuple2<Optional<TaskData>, Optional<TaskData>>>, Boolean>() {

					private static final long serialVersionUID = -2343091659648238232L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<Optional<TaskData>, Optional<TaskData>>> item)
							throws Exception {
						return item._2()._1().isPresent() && !item._2()._2().isPresent();
					}
				}).mapValues(new Function<Tuple2<Optional<TaskData>, Optional<TaskData>>, TaskData>() {

					private static final long serialVersionUID = 3857664450499877227L;

					@Override
					public TaskData call(Tuple2<Optional<TaskData>, Optional<TaskData>> optPair) throws Exception {
						return optPair._1().get();
					}
				});

		// Filter out attributes that cannot find tracks to match.
		JavaPairDStream<String, TaskData> unjoinedAttrStream = unsurelyJoinedStream
				.filter(new Function<Tuple2<String, Tuple2<Optional<TaskData>, Optional<TaskData>>>, Boolean>() {

					private static final long serialVersionUID = -2343091659648238232L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<Optional<TaskData>, Optional<TaskData>>> item)
							throws Exception {
						return !item._2()._1().isPresent() && item._2()._2().isPresent();
					}
				}).mapValues(new Function<Tuple2<Optional<TaskData>, Optional<TaskData>>, TaskData>() {

					private static final long serialVersionUID = 3857664450499877227L;

					@Override
					public TaskData call(Tuple2<Optional<TaskData>, Optional<TaskData>> optPair) throws Exception {
						return optPair._2().get();
					}
				});

		JavaPairDStream<String, Tuple2<Optional<TaskData>, TaskData>> unsurelyJoinedAttrStream = unjoinedTrackStream
				.window(Durations.milliseconds(bufDuration)).rightOuterJoin(unjoinedAttrStream);

		JavaPairDStream<String, Tuple2<TaskData, TaskData>> lateAttrJoinedStream = unsurelyJoinedAttrStream
				.filter(new Function<Tuple2<String, Tuple2<Optional<TaskData>, TaskData>>, Boolean>() {

					private static final long serialVersionUID = -1409792772848000292L;

					@Override
					public Boolean call(Tuple2<String, Tuple2<Optional<TaskData>, TaskData>> item) throws Exception {
						return item._2()._1().isPresent();
					}
				}).mapValues(new Function<Tuple2<Optional<TaskData>, TaskData>, Tuple2<TaskData, TaskData>>() {

					private static final long serialVersionUID = -987544890521949611L;

					@Override
					public Tuple2<TaskData, TaskData> call(Tuple2<Optional<TaskData>, TaskData> item) throws Exception {
						return new Tuple2<TaskData, TaskData>(item._1().get(), item._2());
					}
				});

		JavaPairDStream<String, Tuple2<TaskData, TaskData>> lateTrackJoinedStream = unjoinedTrackStream
				.join(unsurelyJoinedAttrStream
						.filter(new Function<Tuple2<String, Tuple2<Optional<TaskData>, TaskData>>, Boolean>() {

							private static final long serialVersionUID = 1606288045822251826L;

							@Override
							public Boolean call(Tuple2<String, Tuple2<Optional<TaskData>, TaskData>> item)
									throws Exception {
								return !item._2()._1().isPresent();
							}
						}).mapValues(new Function<Tuple2<Optional<TaskData>, TaskData>, TaskData>() {

							private static final long serialVersionUID = 5968796978685130832L;

							@Override
							public TaskData call(Tuple2<Optional<TaskData>, TaskData> item) throws Exception {
								return item._2();
							}
						}).window(Durations.milliseconds(bufDuration)));

		// Union the three track and attribute streams and assemble
		// their TaskData.
		JavaPairDStream<String, TaskData> asmTrackWithAttrStream = instantlyJoinedStream.union(lateTrackJoinedStream)
				.union(lateAttrJoinedStream)
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<TaskData, TaskData>>, String, TaskData>() {

					private static final long serialVersionUID = -4988916606490559467L;

					@Override
					public Tuple2<String, TaskData> call(Tuple2<String, Tuple2<TaskData, TaskData>> pack)
							throws Exception {
						String taskID = pack._1().split(":")[0];
						TaskData taskDataWithTrack = pack._2()._1();
						TaskData taskDataWithAttr = pack._2()._2();
						ExecutionPlan asmPlan = taskDataWithTrack.executionPlan.combine(taskDataWithAttr.executionPlan);

						TaskData asmTaskData = new TaskData(taskDataWithTrack.currentNodeID, asmPlan,
								new PedestrianInfo((Track) taskDataWithTrack.predecessorResult,
										(Attributes) taskDataWithAttr.predecessorResult));
						if (verbose) {
							System.out.println("|INFO|Assembled track and attr of " + pack._1());
						}
						return new Tuple2<String, TaskData>(taskID, asmTaskData);
					}
				});

		// Read track with attribute bytes in parallel from Kafka.
		// Recover attributes from the bytes and extract the ID of the track the
		// attributes belong to.
		JavaPairDStream<String, TaskData> integralTrackWithAttrStream = buildBytesDirectInputStream(streamingContext,
				numRecvStreams, kafkaParams, trackWithAttrTopicMap).mapValues(new Function<byte[], TaskData>() {

					private static final long serialVersionUID = -2623558351815025906L;

					@Override
					public TaskData call(byte[] bytes) throws Exception {
						return (TaskData) SerializationHelper.deserialize(bytes);
					}
				});

		// Union the two track with attribute streams and perform ReID.
		integralTrackWithAttrStream.union(asmTrackWithAttrStream)
				.foreachRDD(new VoidFunction<JavaPairRDD<String, TaskData>>() {

					private static final long serialVersionUID = 8679793229722440129L;

					@Override
					public void call(JavaPairRDD<String, TaskData> trackWithAttrRDD) throws Exception {

						final ObjectSupplier<PedestrianReIDer> reIDerSupplier = reIDerSingleton
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
								PedestrianInfo trackWithAttr = (PedestrianInfo) taskData.predecessorResult;

								// Perform ReID.
								int[] idRank = reIDerSupplier.get().reid(trackWithAttr);

								// Prepare new task data with the pedestrian ID.
								taskData.predecessorResult = idRank;
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
										String rankStr = "";
										for (int id : idRank) {
											rankStr = rankStr + id + " ";
										}
										loggerSupplier.get()
												.info(APP_NAME + ": Sent to Kafka <" + metadata.topic() + "-"
														+ metadata.partition() + "-" + metadata.offset() + "> :"
														+ taskID + ": Pedestrian ID rank: " + rankStr);
									}
								}

								// Always send to the meta data saving
								// application.
								Future<RecordMetadata> future = producerSupplier.get()
										.send(new ProducerRecord<String, byte[]>(
												DataManagingApp.PEDESTRIAN_ID_FOR_SAVING_TOPIC, taskID,
												SerializationHelper.serialize(idRank)));
								RecordMetadata metadata = future.get();
								if (verbose) {
									String rankStr = "";
									for (int id : idRank) {
										rankStr = rankStr + id + " ";
									}
									loggerSupplier.get()
											.info(APP_NAME + ": Sent to Kafka: <" + metadata.topic() + "-"
													+ metadata.partition() + "-" + metadata.offset() + ">" + taskID
													+ ": Pedestrian ID rank: " + rankStr);
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
		PedestrianReIDUsingAttrApp app = new PedestrianReIDUsingAttrApp(propertyCenter);
		app.initialize(propertyCenter);
		app.start();
		app.awaitTermination();
	}
}
