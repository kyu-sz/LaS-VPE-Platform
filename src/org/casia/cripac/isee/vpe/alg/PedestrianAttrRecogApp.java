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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.attr.FakePedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.KafkaSink;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class PedestrianAttrRecogApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = 3104859533881615664L;
	private static final String APPLICATION_NAME = "PedestrianAttributeRecognizing";
	private String sparkMaster;
	private String kafkaBrokers;
	private HashSet<String> topicsSet = new HashSet<>();
	private Properties attrProducerProperties = null;

	public static final String PEDESTRIAN_ATTR_RECOG_TASK_TOPIC = "pedestrian-attr-recog-task";
	public static final String PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC = "pedestrian-attr-recog-input";
	
	private class PedestrianAttributeRecognizerSink implements Serializable {
		
		private static final long serialVersionUID = 1031852129274071157L;
		private PedestrianAttrRecognizer recognizer = null;
		
		public Attribute recognize(Track track) {
			if (recognizer == null) {
				recognizer = new FakePedestrianAttrRecognizer();
			}
			
			return recognizer.recognize(track);
		}
	}

	public PedestrianAttrRecogApp(String sparkMaster, String kafkaBrokers) {
		super();
		
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		
		topicsSet.add(PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);
		
		attrProducerProperties = new Properties();
		attrProducerProperties.put("bootstrap.servers", kafkaBrokers);
		attrProducerProperties.put("producer.type", "sync");
		attrProducerProperties.put("request.required.acks", "1");
		attrProducerProperties.put("compression.codec", "gzip");
		attrProducerProperties.put(
				"key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		attrProducerProperties.put(
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
				sparkContext.broadcast(new KafkaSink<String, byte[]>(attrProducerProperties));
		//Create ResourceSink for any other unserializable components.
		final Broadcast<PedestrianAttributeRecognizerSink> resouceSink =
				sparkContext.broadcast(new PedestrianAttributeRecognizerSink());
		
		//Retrieve tracks from Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("group.id", "1");
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		JavaPairInputDStream<String, byte[]> tracksWithExecQueueDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, topicsSet);
		
		//Recognize attributes from the tracks, then send them to the metadata saving application.
		tracksWithExecQueueDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -1269453288342585510L;

			@Override
			public void call(JavaPairRDD<String, byte[]> tracksWithExecQueueRDD) throws Exception {
				
				tracksWithExecQueueRDD.foreach(new VoidFunction<Tuple2<String,byte[]>>() {

					private static final long serialVersionUID = 6465526220612689594L;

					@Override
					public void call(Tuple2<String, byte[]> trackWithExecQueue) throws Exception {
						String execQueue = trackWithExecQueue._1();
						Track track = (Track) ObjectFactory.getObject(trackWithExecQueue._2());
						
						//Recognize attributes.
						Attribute attribute = resouceSink.value().recognize(track);
						byte[] bytes = ObjectFactory.getByteArray(attribute);
						
						KafkaSink<String, byte[]> producerSink = broadcastKafkaSink.value();
						
						if (execQueue.length() > 0) {
							//Extract current tasks.
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
								broadcastKafkaSink.value().send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												bytes));
								System.out.printf(
										"PedestrianAttrRecogApp: Sent to Kafka: <%s>%s=%s\n",
										topic,
										restExecQueue,
										"An attribute.");
							}
						}
						
						//Always send to the meta data saving application.
						producerSink.send(
								new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC, 
										bytes));
						System.out.printf(
								"PedestrianTrackingApp: Sent to Kafka: <%s>%s\n", 
								MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC,
								"A track");
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
		PedestrianAttrRecogApp pedestrianAttrRecogApp =
				new PedestrianAttrRecogApp(propertyCenter.sparkMaster, propertyCenter.kafkaBrokers);
		pedestrianAttrRecogApp.initialize(propertyCenter.checkpointDir);
		pedestrianAttrRecogApp.start();
		pedestrianAttrRecogApp.awaitTermination();
	}
}
