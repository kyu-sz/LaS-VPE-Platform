package org.isee.vpe.alg.tracking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.isee.vpe.common.SparkStreamingApplication;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class PedestrianTrackingApplication extends SparkStreamingApplication {
	public static final String TRACKING_TASK_TOPIC = "tracking-task";
	public final static String APPLICATION_NAME = "PedestrianTracking";
	
	private static final long serialVersionUID = 3104859533881615664L;
	private String master;
	private String brokers;
	private HashSet<String> topicsSet = new HashSet<>();

	public PedestrianTrackingApplication(String master, String brokers) {
		super();
		
		this.master = master;
		this.brokers = brokers;
		
		topicsSet.add(TRACKING_TASK_TOPIC);
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		SparkConf sparkConf = new SparkConf()
				.setMaster(master)
				.setAppName(APPLICATION_NAME);
		final JavaStreamingContext commandHandlingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(commandHandlingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		JavaDStream<String> linesDStream = messagesDStream.map(
				new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 5410585675756968997L;

					@Override
					public String call(Tuple2<String, String> tuple2) throws Exception {
						System.out.println(tuple2._1() + ":" + tuple2._2());
						return tuple2._2();
					}
				});

		linesDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaRDD<String> linesRDD) throws Exception {
				linesRDD.foreach(new VoidFunction<String>() {
					private static final long serialVersionUID = 7107437032125778866L;

					@Override
					public void call(String commandLine) throws Exception {
						String[] elements = commandLine.split(" ");
						assert (elements.length == 2);
						System.out.println("Tracker: Tracking " + elements[0] + " for " + elements[1] + "...");
					}
				});
			}
		});
		
		return commandHandlingContext;
	}

	/**
	 * @param args No options supported currently.
	 */
	public static void main(String[] args) {
		PedestrianTrackingApplication pedestrianTrackingApplication =
				new PedestrianTrackingApplication("local[*]", "localhost:9092");
		pedestrianTrackingApplication.initialize("checkpoint");
		pedestrianTrackingApplication.start();
		pedestrianTrackingApplication.awaitTermination();
	}
}
