package org.isee.vpe;
/**
 * 
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.admin.TopicCommand;
import kafka.javaapi.producer.Producer;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * @author ken
 * 
 */
public class MainController implements Serializable {
	private static final long serialVersionUID = 4145646848594916984L;
	/**
	 * <br>A list of one or more kafka brokers, in a form of [(broker-host-ip:port,)*broker-host-ip:port].</br>
	 * <br>Example: 192.168.0.111:9999,192.168.0.112:9998</br>
	 */
	String zookeeper = null;
	String brokers = null;
	int partitions;
	int replicateFactor;
	ArrayList<String> topics = new ArrayList<>();
	
	MainController() {
		// TODO Read system configuration from a file.
		zookeeper = "192.168.0.102:2181";
		brokers = "192.168.0.102:9092,192.168.0.100:9092";
		partitions = 20;
		topics.add("Task");
		
		//Create topics.
		for (String topic : topics) {
			CreateTopic(topic);
		}
	}
	
	void CreateTopic(String topic) {
		String[] options = new String[]{
				"--create",
				"--zookeeper",
				zookeeper,
				"--partitions",
				new Integer(partitions).toString(),
				"--topic",
				topic,
				"--replicate-factor",
				new Integer(replicateFactor).toString(),
		};
		TopicCommand.main(options);
	}
	
	JavaStreamingContext getCommandProcessorContext() {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("VideoParsingAndEvaluation");
		JavaStreamingContext commandProcessorContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		Set<String> topicsSet = new HashSet<>(topics);
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		JavaPairInputDStream<String, String> messages =
				KafkaUtils.createDirectStream(commandProcessorContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		JavaDStream<String> lines = messages.map(
				new Function<Tuple2<String, String>, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 5410585675756968997L;

					@Override
					public String call(Tuple2<String, String> tuple2) throws Exception {
						return tuple2._2();
					}
				});
		JavaDStream<String> words = lines.flatMap(
				new FlatMapFunction<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 2163097045190466594L;

					@Override
					public Iterable<String> call(String x) throws Exception {
						return Arrays.asList(x.split(" "));
					}
				});
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 330499059278062281L;

					@Override
					public Tuple2<String, Integer> call(String s) throws Exception {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -1020028924638130443L;

					@Override
					public Integer call(Integer i1, Integer i2) throws Exception {
						return i1 + i2;
					}
				});
		wordCounts.print();
		
		return commandProcessorContext;
	}
	
	void run() {
		JavaStreamingContext commandProcessorContext = getCommandProcessorContext();
		commandProcessorContext.start();
		
		Properties commandProducerProperties = new Properties();
		commandProducerProperties.put("zk.connect", "192.168.0.102:2181");
		commandProducerProperties.put("serializer.class", "kafka.serializer.StringEncoder"); 
		KafkaProducer commandProducer = new KafkaProducer<String, String>(commandProducerProperties);
		
		commandProcessorContext.awaitTermination();
		commandProcessorContext.close();
	}
	
	/**
	 * @param args No options supported currently.
	 */
	public static void main(String[] args) {
		MainController controller = new MainController();
		controller.run();
	}

}
