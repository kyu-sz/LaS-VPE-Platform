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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.KafkaSink;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.casia.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class PedestrianAttrRecogApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = 3104859533881615664L;
	private static final String APPLICATION_NAME = "PedestrianAttributeRecognizing";
	private String sparkMaster;
	private String kafkaBrokers;
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
		
		//Common kafka settings.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("group.id", "1");
		kafkaParams.put("metadata.broker.list", kafkaBrokers);

		//Retrieve tracks from Kafka.
		HashSet<String> inputTopicsSet = new HashSet<>();
		inputTopicsSet.add(PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);
		JavaPairInputDStream<String, byte[]> trackBytesWithExecQueueDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, inputTopicsSet);
		JavaPairDStream<String,Track> trackWithExecQueueDStream =
			trackBytesWithExecQueueDStream.mapToPair(new PairFunction<Tuple2<String,byte[]>, String, Track>() {

				private static final long serialVersionUID = -3502439375476252938L;

				@Override
				public Tuple2<String, Track> call(Tuple2<String, byte[]> trackBytesWithExecQueue) throws Exception {
					return new Tuple2<String, Track>(
							trackBytesWithExecQueue._1(),
							(Track) ObjectFactory.getObject(trackBytesWithExecQueue._2()));
				}
			});
		
		//Retrieve tracks from database.
		FakeDatabaseConnector databaseConnector = new FakeDatabaseConnector();
		HashSet<String> taskTopicsSet = new HashSet<>();
		taskTopicsSet.add(PEDESTRIAN_ATTR_RECOG_TASK_TOPIC);
		JavaPairInputDStream<String, String> videoURLAndTrackIDWithExecQueueDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, taskTopicsSet);
		JavaPairDStream<String,Track> trackWithExecQueueDStreamFromDB = 
				videoURLAndTrackIDWithExecQueueDStream.flatMapToPair(
						new PairFlatMapFunction<Tuple2<String,String>, String, Track>() {

							private static final long serialVersionUID = -3909844904024732450L;

							@Override
							public Iterable<Tuple2<String, Track>> call(Tuple2<String, String> videoURLAndTrackIDWithExecQueue) throws Exception {
								String videoURLAndTrackIDs = videoURLAndTrackIDWithExecQueue._2();
								String[] parts = videoURLAndTrackIDs.split(":");
								String videoURL = parts[0];
								String[] trackIDs = parts[1].split(",");
								Set<Track> tracks = databaseConnector.getTracks(
										videoURLAndTrackIDWithExecQueue._2(),
										trackIDs);
								HashSet<Tuple2<String, Track>> unitedSet = new HashSet<>();
								for (Track track : tracks) {
									unitedSet.add(new Tuple2<String, Track>(videoURL, track));
								}
								return unitedSet;
							}
							
				});
		
		JavaPairDStream<String,Track> unitedTrackWithExecQueueDStream = 
				streamingContext.union(
						trackWithExecQueueDStream,
						Arrays.asList(trackWithExecQueueDStreamFromDB));
		
		//Recognize attributes from the tracks, then send them to the metadata saving application.
		unitedTrackWithExecQueueDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Track>>() {

			private static final long serialVersionUID = -1269453288342585510L;

			@Override
			public void call(JavaPairRDD<String, Track> tracksWithExecQueueRDD) throws Exception {
				
				tracksWithExecQueueRDD.foreach(new VoidFunction<Tuple2<String, Track>>() {

					private static final long serialVersionUID = 6465526220612689594L;

					@Override
					public void call(Tuple2<String, Track> trackWithExecQueue) throws Exception {
						String execQueue = trackWithExecQueue._1();
						Track track = trackWithExecQueue._2();
						
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
								System.out.printf(
										"PedestrianAttrRecogApp: Sending to Kafka: <%s>%s=%s\n",
										topic,
										restExecQueue,
										"An attribute.");
								broadcastKafkaSink.value().send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												bytes));
							}
						}
						
						//Always send to the meta data saving application.
						System.out.printf(
								"PedestrianTrackingApp: Sending to Kafka: <%s>%s\n", 
								MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC,
								"Attribute to save");
						producerSink.send(
								new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC, 
										bytes));
						
						System.gc();
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
