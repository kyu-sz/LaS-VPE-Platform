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

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory.ByteArrayQueueParts;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.casia.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class PedestrianAttrRecogApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = 3104859533881615664L;
	private Properties attrProducerProperties = null;
	private transient SparkConf sparkConf;
	private Map<String, String> kafkaParams = new HashMap<>();

	public static final String APPLICATION_NAME = "PedestrianAttributeRecognizing";
	public static final String PEDESTRIAN_ATTR_RECOG_TASK_TOPIC = "pedestrian-attr-recog-task";
	public static final String PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC = "pedestrian-attr-recog-input";

	public PedestrianAttrRecogApp(SystemPropertyCenter propertyCenter) {
		super();
		
		attrProducerProperties = new Properties();
		attrProducerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		attrProducerProperties.put("producer.type", "sync");
		attrProducerProperties.put("request.required.acks", "1");
		attrProducerProperties.put("compression.codec", "gzip");
		attrProducerProperties.put(
				"key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		attrProducerProperties.put(
				"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		//Create contexts.
		sparkConf = new SparkConf()
				.setAppName(APPLICATION_NAME);
		// Use fair sharing between jobs. 
		sparkConf = sparkConf.set("spark.scheduler.mode", "FAIR");
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}
		
		//Common kafka settings.
		kafkaParams.put("group.id", "1");
		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLogLevel("WARN");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		//Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> broadcastKafkaSink =
				new BroadcastSingleton<>(
						new KafkaProducerFactory<String, byte[]>(attrProducerProperties),
						KafkaProducer.class);
		//Create ResourceSink for any other unserializable components.
		final BroadcastSingleton<PedestrianAttrRecognizer> attrRecognizerSingleton =
				new BroadcastSingleton<>(new ObjectFactory<PedestrianAttrRecognizer>() {

					private static final long serialVersionUID = -5422299243899032592L;

					@Override
					public PedestrianAttrRecognizer getObject() {
						return new FakePedestrianAttrRecognizer();
					}
				}, PedestrianAttrRecognizer.class);

		//Retrieve data from Kafka.
		HashSet<String> inputTopicsSet = new HashSet<>();
		inputTopicsSet.add(PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);
		JavaPairInputDStream<String, byte[]> trackBytesDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, inputTopicsSet);
		
		//Extract tracks from the data.
		JavaPairDStream<String, Tuple2<Track, byte[]>> trackDStreamFromKafka =
		trackBytesDStream.mapValues(new Function<byte[], Tuple2<Track, byte[]>>() {

			private static final long serialVersionUID = -2138675698164723884L;

			@Override
			public Tuple2<Track, byte[]> call(byte[] byteStream) throws Exception {
				byte[] decompressed = ByteArrayFactory.decompress(byteStream);
				ByteArrayQueueParts parts = ByteArrayFactory.splitByteStream(decompressed);
				Track track = (Track) ByteArrayFactory.getObject(parts.head);
				return new Tuple2<Track, byte[]>(track, parts.rest);
			}
		});
		
		//Retrieve tracks from database.
		FakeDatabaseConnector databaseConnector = new FakeDatabaseConnector();
		HashSet<String> taskTopicsSet = new HashSet<>();
		taskTopicsSet.add(PEDESTRIAN_ATTR_RECOG_TASK_TOPIC);
		//Get tasks from Kafka. 
		JavaPairInputDStream<String, byte[]> taskDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, taskTopicsSet);
		
		JavaPairDStream<String, Tuple2<Track, byte[]>> trackDStreamFromTask =
		taskDStream.mapValues(new Function<byte[], Tuple2<Track, byte[]>>() {

			private static final long serialVersionUID = 2423448095433148528L;

			@Override
			public Tuple2<Track, byte[]> call(byte[] compressedDataQueue) throws Exception {
				byte[] dataQueue = ByteArrayFactory.decompress(compressedDataQueue);
				ByteArrayQueueParts dataQueueParts = ByteArrayFactory.splitByteStream(dataQueue);
				String trackInfo = (String) ByteArrayFactory.getObject(dataQueueParts.head);
				String[] trackInfoParts = trackInfo.split(":");
				String videoURL = trackInfoParts[0];
				String trackID = trackInfoParts[1];
				
				Track track = databaseConnector.getTracks(videoURL, trackID);
				return new Tuple2<Track, byte[]>(track, dataQueueParts.rest);
			}
		});
		
		JavaPairDStream<String,Tuple2<Track, byte[]>> unitedTrackDStream = 
				streamingContext.union(
						trackDStreamFromKafka,
						Arrays.asList(trackDStreamFromTask));
		
		//Recognize attributes from the tracks, then send them to the metadata saving application.
		unitedTrackDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Track, byte[]>>>() {

			private static final long serialVersionUID = -1269453288342585510L;

			@Override
			public void call(JavaPairRDD<String, Tuple2<Track, byte[]>> trackRDD) throws Exception {
				
				final ObjectSupplier<PedestrianAttrRecognizer> recognizerSupplier = 
						attrRecognizerSingleton.getSupplier(new JavaSparkContext(trackRDD.context()));
				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier =
						broadcastKafkaSink.getSupplier(new JavaSparkContext(trackRDD.context()));
				
				trackRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Track, byte[]>>>() {

					private static final long serialVersionUID = 6465526220612689594L;

					@Override
					public void call(Tuple2<String, Tuple2<Track, byte[]>> trackWithQueues) throws Exception {
						String execQueue = trackWithQueues._1();
						Track track = trackWithQueues._2()._1();
						byte[] dataQueue = trackWithQueues._2()._2();
						
						//Recognize attributes.
						Attribute attribute = recognizerSupplier.get().recognize(track);
						
						//Prepare new data queue.
						byte[] resBytes = ByteArrayFactory.getByteArray(attribute);
						byte[] compressedResBytes = ByteArrayFactory.getByteArray(resBytes);
						byte[] newDataQueue = ByteArrayFactory.combineByteArray(
								ByteArrayFactory.appendLengthToHead(resBytes), dataQueue);
						byte[] compressedDataQueue = ByteArrayFactory.compress(newDataQueue);
						
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
								producerSupplier.get().send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												compressedDataQueue));
							}
						}
						
						//Always send to the meta data saving application.
						System.out.printf(
								"PedestrianTrackingApp: Sending to Kafka: <%s>%s\n", 
								MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC,
								"Attribute to save");
						producerSupplier.get().send(
								new ProducerRecord<String, byte[]>(
										MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC, 
										compressedResBytes));
						
						System.gc();
					}					
				});
			}
		});
		
		return streamingContext;
	}

	/**
	 * @param args No options supported currently.
	 * @throws URISyntaxException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 */
	public static void main(String[] args) throws URISyntaxException, ParserConfigurationException, SAXException {
		//Load system properties.
		SystemPropertyCenter propertyCenter;
		propertyCenter = new SystemPropertyCenter(args);
		
		TopicManager.checkTopics(propertyCenter);
		
		//Start the pedestrian tracking application.
		PedestrianAttrRecogApp pedestrianAttrRecogApp = new PedestrianAttrRecogApp(propertyCenter);
		pedestrianAttrRecogApp.initialize(propertyCenter);
		pedestrianAttrRecogApp.start();
		pedestrianAttrRecogApp.awaitTermination();
	}
}
