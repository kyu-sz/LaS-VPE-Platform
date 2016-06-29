package org.isee.vpe.alg;

import java.io.IOException;
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
import org.isee.pedestrian.tracking.FakePedestrianTracker;
import org.isee.pedestrian.tracking.PedestrianTracker;
import org.isee.pedestrian.tracking.PedestrianTracker.Track;
import org.isee.vpe.common.KafkaSink;
import org.isee.vpe.common.SparkStreamingApp;
import org.isee.vpe.common.SystemPropertyCenter;

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
	private PedestrianTracker tracker = null;
	private Properties trackingResultProducerProperties = null;

	public PedestrianTrackingApp(String sparkMaster, String kafkaBrokers) {
		super();
		
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		
		topicsSet.add(TRACKING_TASK_TOPIC);
		
		tracker = new FakePedestrianTracker();
		
		trackingResultProducerProperties = new Properties();
		trackingResultProducerProperties.put("bootstrap.servers", kafkaBrokers);
		trackingResultProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingResultProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
		final Broadcast<KafkaSink<String, String>> broadcastKafkaSink =
				sparkContext.broadcast(new KafkaSink<String, String>(trackingResultProducerProperties));
		
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
				return tracker.track(videoURL);
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
						KafkaSink<String, String> producerSink = broadcastKafkaSink.value();
						producerSink.send(new ProducerRecord<String, String>(TRACKING_RESULT_TOPIC, "Hello"));
						System.out.printf("Sent to Kafka: <%s>%s\n", TRACKING_RESULT_TOPIC, "Hello");
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
