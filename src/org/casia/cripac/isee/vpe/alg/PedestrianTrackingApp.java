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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory.ByteArrayQueueParts;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakePedestrianTracker;

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
	private HashSet<String> taskTopicsSet = new HashSet<>();
	private Properties trackProducerProperties = new Properties();
	private transient SparkConf sparkConf;
	private Map<String, String> kafkaParams = new HashMap<>();

	public static final String APPLICATION_NAME = "PedestrianTracking";
	public static final String PEDESTRIAN_TRACKING_TASK_TOPIC = "tracking-task";

	public PedestrianTrackingApp(SystemPropertyCenter propertyCenter) {
		super();
		
		taskTopicsSet.add(PEDESTRIAN_TRACKING_TASK_TOPIC);
		
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
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}

		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.	
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLogLevel("WARN");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		//Create KafkaSink for Spark Streaming to output to Kafka.
		final Broadcast<ObjectSupplier<KafkaProducer<String, byte[]>>> broadcastKafkaSink =
				sparkContext.broadcast(
						new ObjectSupplier<KafkaProducer<String, byte[]>>(
								new KafkaProducerFactory<>(trackProducerProperties)));
		//Create ResourceSink for any other unserializable components.
		final Broadcast<ObjectSupplier<PedestrianTracker>> trackerSink =
				sparkContext.broadcast(new ObjectSupplier<PedestrianTracker>(new ObjectFactory<PedestrianTracker>() {

					private static final long serialVersionUID = -3454317350293711609L;

					@Override
					public PedestrianTracker getObject() {
						return new FakePedestrianTracker();
					}
				}));
		
		//Retrieve messages from Kafka.
		JavaPairInputDStream<String, byte[]> taskDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, taskTopicsSet);
		
		//Get video URL from the data queue.
		JavaPairDStream<String, Tuple2<String, byte[]>> videoURLDStream = taskDStream.mapValues(new Function<byte[], Tuple2<String, byte[]>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, byte[]> call(byte[] compressedDataQueue) throws Exception {
				byte[] dataQueue = ByteArrayFactory.decompress(compressedDataQueue);
				ByteArrayQueueParts parts = ByteArrayFactory.splitByteStream(dataQueue);
				String videoURL = (String) ByteArrayFactory.getObject(parts.head);
				return new Tuple2<String, byte[]>(videoURL, parts.rest);
			}
		});
		
		//Get pedestrian tracks from video at the URL by a pedestrian tracker.
		JavaPairDStream<String,Tuple2<Track, byte[]>> trackDStream =
				videoURLDStream.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<String,byte[]>>, String, Tuple2<Track,byte[]>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String,Tuple2<Track, byte[]>>> call(Tuple2<String,Tuple2<String,byte[]>> videoURLWithDataQueue) throws Exception {
						String taskQueue = videoURLWithDataQueue._1();
						String videoURL = videoURLWithDataQueue._2()._1();
						byte[] dataQueue = videoURLWithDataQueue._2()._2();
						HashSet<Tuple2<String, Tuple2<Track, byte[]>>> unitedResult = new HashSet<>();
						
						Set<Track> tracks = trackerSink.value().get().track(videoURL);
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
						
						KafkaProducer<String, byte[]> producer = broadcastKafkaSink.value().get();

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
								producer.send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												compressedDataQueue));
							}
						}
						
						//Always send to the meta data saving application.
						System.out.printf(
								"PedestrianTrackingApp: Sending to Kafka: <%s>%s\n", 
								MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC,
								"A track");
						producer.send(
								new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC, 
										compressedResBytes));
						
						System.gc();
					}
				});
			}
		});
		
		return streamingContext;
	}

	/**
	 * @param args No options supported currently.
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws URISyntaxException {
		//Load system properties.
		SystemPropertyCenter propertyCenter;
		propertyCenter = new SystemPropertyCenter(args);
		
		TopicManager.checkTopics(propertyCenter);
		
		//Start the pedestrian tracking application.
		PedestrianTrackingApp pedestrianTrackingApp = new PedestrianTrackingApp(propertyCenter);
		pedestrianTrackingApp.initialize(propertyCenter);
		pedestrianTrackingApp.start();
		pedestrianTrackingApp.awaitTermination();
	}
}
