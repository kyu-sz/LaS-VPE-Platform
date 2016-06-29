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
import org.isee.vpe.alg.PedestrianTrackingApp;
import org.isee.vpe.common.KafkaSink;
import org.isee.vpe.common.SparkStreamingApp;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

@SuppressWarnings("unused")
public class MessageHandlingApp extends SparkStreamingApp {
	
	public final static String COMMAND_TOPIC = "command";
	public final static String APPLICATION_NAME = "MessageHandling";

	private static final long serialVersionUID = -942388332211825622L;
	private Pattern spaceSplitter = Pattern.compile(" ");
	private String kafkaBrokers;
	private String sparkMaster;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackingTaskProducerProperties;
	
	public MessageHandlingApp(String sparkMaster, String kafkaBrokers) {
		super();
		
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		System.out.println("Message handler: Kafka brokers: " + kafkaBrokers);
		
		topicsSet.add(COMMAND_TOPIC);
		
		trackingTaskProducerProperties = new Properties();
		trackingTaskProducerProperties.put("bootstrap.servers", kafkaBrokers);
		trackingTaskProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingTaskProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
				sparkContext.broadcast(new KafkaSink<String, String>(trackingTaskProducerProperties));
		
		//Create an input DStream using Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		kafkaParams.put("group.id", "0");
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		//Handle the messages received from Kafka,
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
						switch (command) {
						case "Track":
							String videoURL = params;
							broadcastKafkaSink.value().send(
									new ProducerRecord<String, String>(
											PedestrianTrackingApp.TRACKING_TASK_TOPIC,
											videoURL));
							System.out.printf(
									"Message handler: sent to Kafka <%s>%s\n",
									PedestrianTrackingApp.TRACKING_TASK_TOPIC,
									videoURL);
							break;
						}
					}
				});
			}
		});
		
		return streamingContext;
	}
}
