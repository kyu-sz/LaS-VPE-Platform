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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.tracking.FakePedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.KafkaSink;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

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
	private static final String APPLICATION_NAME = "PedestrianTracking";
	private String sparkMaster;
	private String kafkaBrokers;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackProducerProperties = null;
	
	public static final String PEDESTRIAN_TRACKING_TASK_TOPIC = "tracking-task";
	
	private class ResourceSink implements Serializable {
		private static final long serialVersionUID = 1031852129274071157L;
		private PedestrianTracker tracker = null;
		
		public PedestrianTracker getTracker() {
			if (tracker == null) {
				tracker = new FakePedestrianTracker();
			}
			
			return tracker;
		}
	}

	public PedestrianTrackingApp(String sparkMaster, String kafkaBrokers) {
		super();
		
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		
		topicsSet.add(PEDESTRIAN_TRACKING_TASK_TOPIC);
		
		trackProducerProperties = new Properties();
		trackProducerProperties.put("bootstrap.servers", kafkaBrokers);
		trackProducerProperties.put("producer.type", "sync");
		trackProducerProperties.put("request.required.acks", "1");
		trackProducerProperties.put("compression.codec", "gzip");
		trackProducerProperties.put(
				"key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackProducerProperties.put(
				"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		SparkConf sparkConf = new SparkConf()
				.setMaster(sparkMaster)
				.setAppName(APPLICATION_NAME);		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		//Create KafkaSink for Spark Streaming to output to Kafka.
		final Broadcast<KafkaSink<String, byte[]>> broadcastKafkaSink =
				sparkContext.broadcast(new KafkaSink<String, byte[]>(trackProducerProperties));
		//Create ResourceSink for any other unserializable components.
		final Broadcast<ResourceSink> resouceSink =
				sparkContext.broadcast(new ResourceSink());
		
		//Retrieve messages from Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		JavaPairInputDStream<String, String> videoURLWithExecQueueDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		//Get pedestrian tracks from videos at the URLs by a pedestrian tracker.
		JavaDStream<Tuple2<String, Track>> tracksWithExecQueueDStream = videoURLWithExecQueueDStream.flatMap(
				new FlatMapFunction<Tuple2<String, String>, Tuple2<String, Track>>() {
					private static final long serialVersionUID = -3035821562428112978L;
					
					@Override
					public Iterable<Tuple2<String, Track>> call(Tuple2<String, String> videoURL) throws Exception {
						HashSet<Tuple2<String, Track>> unitedResult = new HashSet<>();
						
						Set<Track> tracks = resouceSink.value().getTracker().track(videoURL._2());
						for (Track track : tracks) {
							unitedResult.add(new Tuple2<String, Track>(videoURL._1(), track));
						}
						
						return unitedResult;
					}
		});
		
		//Send the tracks to the Kafka.
		tracksWithExecQueueDStream.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, Track>>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaRDD<Tuple2<String, Track>> tracksWithExecQueueRDD) throws Exception {
				
				tracksWithExecQueueRDD.foreach(new VoidFunction<Tuple2<String, Track>>() {
					
					private static final long serialVersionUID = 7107437032125778866L;

					@Override
					public void call(Tuple2<String, Track> trackWithExecQueue) throws Exception {
						String execQueue = trackWithExecQueue._1();
						Track track = trackWithExecQueue._2();
						
						//Transform the track into byte[]
						byte[] bytes = ObjectFactory.getByteArray(track);
						
						//TODO Modify here to get a producer from the sink and use it directly.
						KafkaSink<String, byte[]> producerSink = broadcastKafkaSink.value();

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
								producerSink.send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												bytes));
							}
						}
						
						//Always send to the metadata saving application.
						System.out.printf(
								"PedestrianTrackingApp: Sending to Kafka: <%s>%s\n", 
								MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC,
								"A track");
						producerSink.send(
								new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC, 
										bytes));
					}
				});
			}
		});
		
		return streamingContext;
	}

	/**
	 * @param args No options supported currently.
	 */
	public static void main(String[] args) {
		//Load system properties.
		SystemPropertyCenter propertyCenter;
		try {
			propertyCenter = new SystemPropertyCenter("system.properties");
		} catch (IOException e) {
			e.printStackTrace();
			propertyCenter = new SystemPropertyCenter();
		}
		
		//Start the pedestrian tracking application.
		PedestrianTrackingApp pedestrianTrackingApp =
				new PedestrianTrackingApp(propertyCenter.sparkMaster, propertyCenter.kafkaBrokers);
		pedestrianTrackingApp.initialize(propertyCenter.checkpointDir);
		pedestrianTrackingApp.start();
		pedestrianTrackingApp.awaitTermination();
	}
}
