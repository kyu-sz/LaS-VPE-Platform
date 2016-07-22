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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory.ByteArrayQueueParts;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The PedestrianTrackingApp class takes in video URLs from Kafka,
 * then process the videos with pedestrian tracking algorithms,
 * and finally push the tracking results back to Kafka.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianTrackingApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = 3104859533881615664L;
//	private HashSet<String> taskTopicsSet = new HashSet<>();
	private Map<String, Integer> taskTopicPartitions = new HashMap<>();
	private Properties trackProducerProperties = new Properties();
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private boolean verbose = false;
	
	String messageListenerAddr;
	int messageListenerPort;

	public static final String APPLICATION_NAME = "PedestrianTracking";
	public static final String PEDESTRIAN_TRACKING_TASK_TOPIC = "tracking-task";

	public PedestrianTrackingApp(SystemPropertyCenter propertyCenter) {
		super();
		
		verbose = propertyCenter.verbose;
		
		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;
		
//		taskTopicsSet.add(PEDESTRIAN_TRACKING_TASK_TOPIC);
		taskTopicPartitions.put(PEDESTRIAN_TRACKING_TASK_TOPIC, propertyCenter.kafkaPartitions);
		
		trackProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		trackProducerProperties.put("producer.type", "sync");
		trackProducerProperties.put("request.required.acks", "1");
		trackProducerProperties.put("compression.codec", "gzip");
		trackProducerProperties.put(
				"key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackProducerProperties.put(
				"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

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
				.set("spark.storage.memoryFraction", "1");
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
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
		//Create contexts.	
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLogLevel("WARN");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		//Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> producerSingleton =
				new BroadcastSingleton<KafkaProducer<String, byte[]>>(
						new KafkaProducerFactory<>(trackProducerProperties),
						KafkaProducer.class);
		//Create ResourceSink for any other unserializable components.
		final BroadcastSingleton<PedestrianTracker> trackerSingleton =
				new BroadcastSingleton<PedestrianTracker>(new ObjectFactory<PedestrianTracker>() {

					private static final long serialVersionUID = -3454317350293711609L;

					@Override
					public PedestrianTracker getObject() {
						return new FakePedestrianTracker();
					}
				}, PedestrianTracker.class);
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
		JavaPairReceiverInputDStream<String, byte[]> taskDStream = KafkaUtils.createStream(
				streamingContext,
				String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
				commonKafkaParams,
				taskTopicPartitions, 
				StorageLevel.MEMORY_AND_DISK_SER());
		
//		//Retrieve messages from Kafka.
//		JavaPairInputDStream<String, byte[]> taskDStream =
//				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
//				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, taskTopicsSet);
		
		//Get video URL from the data queue.
		JavaPairDStream<String, Tuple2<String, byte[]>> videoURLDStream = taskDStream.mapValues(new Function<byte[], Tuple2<String, byte[]>>() {

			private static final long serialVersionUID = 1992431816877590290L;

			@Override
			public Tuple2<String, byte[]> call(byte[] compressedDataQueue) throws Exception {
				byte[] dataQueue = ByteArrayFactory.decompress(compressedDataQueue);
				ByteArrayQueueParts parts = ByteArrayFactory.splitByteStream(dataQueue);
				String videoURL = (String) ByteArrayFactory.getObject(parts.head);
				return new Tuple2<String, byte[]>(videoURL, parts.rest);
			}
		});
		
		videoURLDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<String, byte[]>>>() {

			private static final long serialVersionUID = -5700026946957604438L;

			@Override
			public void call(JavaPairRDD<String, Tuple2<String, byte[]>> videoURLRDD) throws Exception {

				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier =
						producerSingleton.getSupplier(new JavaSparkContext(videoURLRDD.context()));
				final ObjectSupplier<PedestrianTracker> trackerSupplier =
						trackerSingleton.getSupplier(new JavaSparkContext(videoURLRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = 
						loggerSingleton.getSupplier(new JavaSparkContext(videoURLRDD.context()));
				
				videoURLRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,byte[]>>>() {

					private static final long serialVersionUID = 955383087048954689L;

					@Override
					public void call(Tuple2<String, Tuple2<String, byte[]>> videoURLPackage) throws Exception {
						String execQueue = videoURLPackage._1();
						String videoURL = videoURLPackage._2()._1();
						byte[] restDataQueue = videoURLPackage._2()._2();
						
						Set<Track> tracks = trackerSupplier.get().track(videoURL);
						
						for (Track track : tracks) {
							//Append the track to the dataQueue.
							byte[] trackBytes = ByteArrayFactory.getByteArray(track);
							byte[] compressedTrackBytes = ByteArrayFactory.compress(trackBytes);
							byte[] newDataQueue = ByteArrayFactory.combineByteArray(
									ByteArrayFactory.appendLengthToHead(trackBytes), restDataQueue);
							byte[] compressedDataQueue = ByteArrayFactory.compress(newDataQueue);

							if (execQueue.length() > 0) {
								//Extract current execution queue.
								String curExecQueue;
								String restExecQueue;
								int splitIndex = execQueue.indexOf('|');
								if (splitIndex == -1) {
									curExecQueue = execQueue;
									restExecQueue = "";
								} else {
									curExecQueue = execQueue.substring(0, splitIndex);
									restExecQueue = execQueue.substring(splitIndex + 1);
								}
								
								String[] topics = curExecQueue.split(",");
								
								//Send to each topic.
								for (String topic : topics) {
									if (verbose) {
										loggerSupplier.get().info("PedestrianTrackingApp: Sending to Kafka: <" +
												topic + ">" + restExecQueue + "=" + "A track");
									}
									producerSupplier.get().send(
											new ProducerRecord<String, byte[]>(
													topic,
													restExecQueue,
													compressedDataQueue));
								}
							}
							
							//Always send to the meta data saving application.
							if (verbose) {
								loggerSupplier.get().info("PedestrianTrackingApp: Sending to Kafka: <" +
										MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC + ">" + "A track");
							}
							producerSupplier.get().send(
									new ProducerRecord<String, byte[]>(
											MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC, 
											compressedTrackBytes));
							
							System.gc();
						}
					}
				});
			}
		});
		
		/*******************************************************************************
		 * The following codes are discarded because we cannot get a tracker inside a map function.
		 * This is because the tracker should be broadcast, but broadcast is not compatible with checkpointing.
		 * Following the official programming guide, the broadcast itself should be lazy-evaluated.
		 * That means it should be created during the stream if we recover the application.
		 * But we cannot get a Spark context inside a map function.
		 * Using context outside would cause the task to be not serializable.
		 * TODO: Find a workaround here.

		//Get pedestrian tracks from video at the URL by a pedestrian tracker.
		JavaPairDStream<String,Tuple2<Track, byte[]>> trackDStream =
				videoURLDStream.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,byte[]>>, String, Tuple2<Track,byte[]>>() {

					private static final long serialVersionUID = -1754838428126683921L;

					@Override
					public Iterable<Tuple2<String,Tuple2<Track, byte[]>>> call(Tuple2<String,Tuple2<String,byte[]>> videoURLWithDataQueue) throws Exception {
						String taskQueue = videoURLWithDataQueue._1();
						String videoURL = videoURLWithDataQueue._2()._1();
						byte[] dataQueue = videoURLWithDataQueue._2()._2();
						HashSet<Tuple2<String, Tuple2<Track, byte[]>>> unitedResult = new HashSet<>();
						Set<Track> tracks = trackerSingleton.getSupplier(
								new JavaSparkContext(videoURLDStream.context().sc())).get().track(videoURL);
						for (Track track : tracks) {
							unitedResult.add(new Tuple2<String, Tuple2<Track, byte[]>>(
									taskQueue,
									new Tuple2<Track, byte[]>(track, dataQueue)));
						}
						
						return unitedResult;
					}
		});
		
		//Send the tracks to the Kafka.
		trackDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Track, byte[]>>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, Tuple2<Track, byte[]>> trackRDD) throws Exception {
				
				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = 
						producerSingleton.getSupplier(new JavaSparkContext(trackRDD.context()));
				
				trackRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Track, byte[]>>>() {
					
					private static final long serialVersionUID = 7107437032125778866L;

					@Override
					public void call(Tuple2<String, Tuple2<Track, byte[]>> trackAndDataQueueAndExecQueue) throws Exception {
						String execQueue = trackAndDataQueueAndExecQueue._1();
						Track track = trackAndDataQueueAndExecQueue._2()._1();
						byte[] dataQueue = trackAndDataQueueAndExecQueue._2()._2();
						
						//Append the track to the dataQueue.
						byte[] resBytes = ByteArrayFactory.getByteArray(track);
						byte[] compressedResBytes = ByteArrayFactory.compress(resBytes);
						byte[] newDataQueue = ByteArrayFactory.combineByteArray(
								ByteArrayFactory.appendLengthToHead(resBytes), dataQueue);
						byte[] compressedDataQueue = ByteArrayFactory.compress(newDataQueue);

						if (execQueue.length() > 0) {
							//Extract current execution queue.
							String curExecQueue;
							String restExecQueue;
							int splitIndex = execQueue.indexOf('|');
							if (splitIndex == -1) {
								curExecQueue = execQueue;
								restExecQueue = "";
							} else {
								curExecQueue = execQueue.substring(0, splitIndex);
								restExecQueue = execQueue.substring(splitIndex + 1);
							}
							
							String[] topics = curExecQueue.split(",");
							
							//Send to each topic.
							for (String topic : topics) {
								System.out.printf(
										"PedestrianTrackingApp: Sending to Kafka: <%s>%s=%s\n", 
										topic,
										restExecQueue,
										"A track");
								producerSupplier.get().send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												compressedDataQueue));
							}
						}
						
						//Always send to the meta data saving application.
						if (verbose) {
							System.out.printf(
									"PedestrianTrackingApp: Sending to Kafka: <%s>%s\n", 
									MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC,
									"A track");
						}
						producerSupplier.get().send(
								new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC, 
										compressedResBytes));
						
						System.gc();
					}
				});
			}
		});
		*********************************************************************************/
		
		return streamingContext;
	}

	/**
	 * @param args No options supported currently.
	 * @throws URISyntaxException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public static void main(String[] args) throws URISyntaxException, ParserConfigurationException, SAXException {
		//Load system properties.
		SystemPropertyCenter propertyCenter;
		propertyCenter = new SystemPropertyCenter(args);
		
		if (propertyCenter.verbose) {
			System.out.println("Starting PedestrianTrackingApp...");
		}
		
		TopicManager.checkTopics(propertyCenter);
		
		//Start the pedestrian tracking application.
		PedestrianTrackingApp pedestrianTrackingApp = new PedestrianTrackingApp(propertyCenter);
		pedestrianTrackingApp.initialize(propertyCenter);
		pedestrianTrackingApp.start();
		pedestrianTrackingApp.awaitTermination();
	}
}
