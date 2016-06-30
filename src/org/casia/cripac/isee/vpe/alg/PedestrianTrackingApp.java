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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class PedestrianTrackingApp extends SparkStreamingApp {
	
	public static final String TRACKING_TASK_TOPIC = "tracking-task";
	public static final String TRACKING_RESULT_TOPIC = "tracking-result";
	public final static String APPLICATION_NAME = "PedestrianTracking";
	
	private static final long serialVersionUID = 3104859533881615664L;
	private String sparkMaster;
	private String kafkaBrokers;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackingResultProducerProperties = null;
	
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
		
		topicsSet.add(TRACKING_TASK_TOPIC);
		
		trackingResultProducerProperties = new Properties();
		trackingResultProducerProperties.put("bootstrap.servers", kafkaBrokers);
		trackingResultProducerProperties.put("producer.type", "sync");
		trackingResultProducerProperties.put("request.required.acks", "1");
		trackingResultProducerProperties.put("compression.codec", "gzip");
		trackingResultProducerProperties.put(
				"key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingResultProducerProperties.put(
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
				sparkContext.broadcast(new KafkaSink<String, byte[]>(trackingResultProducerProperties));
		//Create ResourceSink for any other unserializable components.
		final Broadcast<ResourceSink> resouceSink =
				sparkContext.broadcast(new ResourceSink());
		
		//Retrieve messages from Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		//Extract video URLs from the message.
		JavaDStream<String> videoURLsDStream = messagesDStream.map(
				new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 5410585675756968997L;

					@Override
					public String call(Tuple2<String, String> tuple2) throws Exception {
						System.out.println(tuple2._1() + ":" + tuple2._2());
						return tuple2._2();
					}
				});
		
		//Get pedestrian tracks from videos at the URLs by a pedestrian tracker.
		JavaDStream<Track> tracksDStream = videoURLsDStream.flatMap(new FlatMapFunction<String, Track>() {

			private static final long serialVersionUID = -3035821562428112978L;

			@Override
			public Iterable<Track> call(String videoURL) throws Exception {
				return resouceSink.value().getTracker().track(videoURL);
			}
		});
		
		//Send the tracks to the Kafka.
		tracksDStream.foreachRDD(new VoidFunction<JavaRDD<Track>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaRDD<Track> tracksRDD) throws Exception {
				tracksRDD.foreach(new VoidFunction<Track>() {
					private static final long serialVersionUID = 7107437032125778866L;

					@Override
					public void call(Track track) throws Exception {
						//Transform the track into byte[]
						ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
						ObjectOutput output = null;
						try {
							output = new ObjectOutputStream(byteArrayOutputStream);
							output.writeObject(track);
							byte[] bytes = byteArrayOutputStream.toByteArray();
							
							KafkaSink<String, byte[]> producerSink = broadcastKafkaSink.value();
							producerSink.send(
									new ProducerRecord<String, byte[]>(
											TRACKING_RESULT_TOPIC, 
											bytes));
							System.out.printf("Sent to Kafka: <%s>%s\n", TRACKING_RESULT_TOPIC, "A track");
						} finally {
							try {
								if (output != null) {
									output.close();
								}
							} catch (IOException e) {
								// ignore close exception
							}
							try {
								byteArrayOutputStream.close();
							} catch (IOException e) {
								// ignore close exception
							}
						}
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
