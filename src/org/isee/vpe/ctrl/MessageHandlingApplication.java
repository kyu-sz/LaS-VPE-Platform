package org.isee.vpe.ctrl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
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
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties trackingTaskProducerProperties;
	
	public MessageHandlingApplication(String brokers) {
		super();
		
		this.brokers = brokers;
		
		topicsSet.add(COMMAND_TOPIC);
		
		trackingTaskProducerProperties = new Properties();
		trackingTaskProducerProperties.put("bootstrap.servers", brokers);
		trackingTaskProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		trackingTaskProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(APPLICATION_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext commandHandlingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		KafkaSink<String, String> kafkaSink = new KafkaSink<String, String>(trackingTaskProducerProperties);
		final Broadcast<KafkaSink<String, String>> broadcastKafkaSink = sparkContext.broadcast(kafkaSink);
		
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(commandHandlingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		JavaDStream<String> linesDStream = messagesDStream.map(
				new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 5410585675756968997L;

					@Override
					public String call(Tuple2<String, String> tuple2) throws Exception {
						System.out.println("Message handler: recerived from Kafka " + tuple2._1() + "-" + tuple2._2());
						return tuple2._2();
					}
				});		
		linesDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaRDD<String> linesRDD) throws Exception {
				
				linesRDD.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(String line) throws Exception {
						String[] elements = spaceSplitter.split(line);
						assert (elements.length > 0);
						switch (elements[0]) {
						case "Track":
							assert (elements.length == 4);
							String videoURL = elements[1];
							String client = elements[3];
							broadcastKafkaSink.value().send(new ProducerRecord<String, String>(PedestrianTrackingApplication.TRACKING_TASK_TOPIC, videoURL + " " + client));
							System.out.println("Message handler: sent to Kafka <" + PedestrianTrackingApplication.TRACKING_TASK_TOPIC + ">" + videoURL + " " + client);
							break;
						}
					}
				});
			}
		});
		
		return commandHandlingContext;
	}
}
