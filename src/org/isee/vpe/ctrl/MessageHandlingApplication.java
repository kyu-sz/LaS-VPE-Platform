package org.isee.vpe.ctrl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.isee.vpe.alg.tracking.PedestrianTrackingApplication;
import org.isee.vpe.common.KafkaSink;
import org.isee.vpe.common.SparkStreamingApplication;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class MessageHandlingApplication extends SparkStreamingApplication {
	
	public final static String COMMAND_TOPIC = "command";
	public final static String APPLICATION_NAME = "CommandHandling";

	private static final long serialVersionUID = -942388332211825622L;
	private Pattern spaceSplitter = Pattern.compile(" ");
	private String brokers;
	private String master;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackingTaskProducerProperties;
	
	public MessageHandlingApplication(String master, String brokers) {
		super();
		
		this.master = master;
		this.brokers = brokers;
		
		topicsSet.add(COMMAND_TOPIC);
		
		trackingTaskProducerProperties = new Properties();
		trackingTaskProducerProperties.put("bootstrap.servers", brokers);
		trackingTaskProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingTaskProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		SparkConf sparkConf = new SparkConf()
				.setMaster(master)
				.setAppName(APPLICATION_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		//Create KafkaSink for Spark Streaming to output to Kafka.
		final Broadcast<KafkaSink<String, String>> broadcastKafkaSink =
				sparkContext.broadcast(new KafkaSink<String, String>(trackingTaskProducerProperties));
		
		//Create an input DStream using Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		messagesDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, String> messagesRDD) throws Exception {
				
				messagesRDD.foreach(new VoidFunction<Tuple2<String, String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, String> message) throws Exception {
						String command = message._1();
						String params = message._2();
						String[] elements = spaceSplitter.split(params);
						switch (command) {
						case "Track":
							assert (elements.length == 4);
							String videoURL = elements[0];
							String client = elements[2];
							broadcastKafkaSink.value().send(new ProducerRecord<String, String>(PedestrianTrackingApplication.TRACKING_TASK_TOPIC, videoURL + " " + client));
							System.out.println("Message handler: sent to Kafka <" + PedestrianTrackingApplication.TRACKING_TASK_TOPIC + ">" + videoURL + " " + client);
							break;
						}
					}
				});
			}
		});
		
		return streamingContext;
	}
}
