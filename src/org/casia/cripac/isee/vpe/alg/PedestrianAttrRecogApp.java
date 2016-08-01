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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.casia.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * The PedestrianAttrRecogApp class is a Spark Streaming application which performs pedestrian attribute recognition.
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianAttrRecogApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = 3104859533881615664L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "PedestrianAttributeRecognizing";
	/**
	 * Topic to input attribute recognition tasks.
	 * The tasks would tell the application to fetch input data from database or hard disks.
	 */
	public static final String PEDESTRIAN_ATTR_RECOG_TASK_TOPIC = "pedestrian-attr-recog-task";
	/**
	 * Topic to input tracks from Kafka.
	 * The application directly performs recognition on these tracks,
	 * rather than fetching input data from other sources.
	 */
	public static final String PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC = "pedestrian-attr-recog-track-input";
	
	/**
	 * Register these topics to the TopicManager, so that on the start of the whole system,
	 * the TopicManager can help register the topics this application needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(PEDESTRIAN_ATTR_RECOG_TASK_TOPIC);
		TopicManager.registerTopic(PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC);
	}
	
	private Properties attrProducerProperties = null;
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private boolean verbose = false;
//	private HashSet<String> inputTopicsSet = new HashSet<>();
//	private HashSet<String> taskTopicsSet = new HashSet<>();
	private Map<String, Integer> inputTopicPartitions = new HashMap<>();
	private Map<String, Integer> taskTopicPartitions = new HashMap<>();
	private String messageListenerAddr;
	private int messageListenerPort;
	private int numRecvStreams;

	public PedestrianAttrRecogApp(SystemPropertyCenter propertyCenter) {
		super();

//		taskTopicsSet.add(PEDESTRIAN_ATTR_RECOG_TASK_TOPIC);
//		inputTopicsSet.add(PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);
		taskTopicPartitions.put(PEDESTRIAN_ATTR_RECOG_TASK_TOPIC, propertyCenter.kafkaPartitions);
		inputTopicPartitions.put(PEDESTRIAN_ATTR_RECOG_TRACK_INPUT_TOPIC, propertyCenter.kafkaPartitions);
		
		verbose = propertyCenter.verbose;
		
		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;
		
		numRecvStreams = propertyCenter.numRecvStreams;
		
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
				.setAppName(APP_NAME)
				.set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");
		
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}
		
		//Common kafka settings.
		commonKafkaParams.put("group.id", "PedestrianAttrRecogApp" + UUID.randomUUID());
		commonKafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		// Determine where the stream starts (default: largest)
		commonKafkaParams.put("auto.offset.reset", "smallest");
		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		commonKafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
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
		final BroadcastSingleton<SynthesizedLogger> loggerSingleton =
				new BroadcastSingleton<>(
						new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort),
						SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by Zookeeper.
		 * TODO Find ways to robustly make use of createDirectStream.
		 */
		List<JavaPairDStream<String, byte[]>> parTrackStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parTrackStreams.add(KafkaUtils.createStream(streamingContext,
					String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
					commonKafkaParams,
					inputTopicPartitions, 
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		JavaPairDStream<String, byte[]> trackStream =
				streamingContext.union(parTrackStreams.get(0), parTrackStreams.subList(1, parTrackStreams.size()));
//		//Retrieve data from Kafka.
//		JavaPairInputDStream<String, byte[]> trackBytesDStream =
//				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
//				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, inputTopicsSet);
		
		//Extract tracks from the data.
		JavaPairDStream<String, Tuple2<Track, byte[]>> trackDStreamFromKafka =
		trackStream.mapValues(new Function<byte[], Tuple2<Track, byte[]>>() {

			private static final long serialVersionUID = -2138675698164723884L;

			@Override
			public Tuple2<Track, byte[]> call(byte[] byteStream) throws Exception {
				byte[] decompressed = ByteArrayFactory.decompress(byteStream);
				ByteArrayQueueParts parts = ByteArrayFactory.splitByteStream(decompressed);
				Track track = (Track) ByteArrayFactory.getObject(parts.head);
				return new Tuple2<Track, byte[]>(track, parts.rest);
			}
		});

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by Zookeeper.
		 * TODO Find ways to robustly make use of createDirectStream.
		 */
		List<JavaPairDStream<String, byte[]>> parTaskStreams = new ArrayList<>(numRecvStreams);
		for (int i = 0; i < numRecvStreams; i++) {
			parTaskStreams.add(KafkaUtils.createStream(streamingContext,
					String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
					commonKafkaParams,
					taskTopicPartitions, 
					StorageLevel.MEMORY_AND_DISK_SER()));
		}
		JavaPairDStream<String, byte[]> taskStream =
				streamingContext.union(parTaskStreams.get(0), parTaskStreams.subList(1, parTaskStreams.size()));
//		//Get tasks from Kafka. 
//		JavaPairInputDStream<String, byte[]> taskDStream =
//				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
//				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, taskTopicsSet);
		
		//Retrieve tracks from database.
		FakeDatabaseConnector databaseConnector = new FakeDatabaseConnector();
		
		JavaPairDStream<String, Tuple2<Track, byte[]>> trackDStreamFromTask =
		taskStream.mapValues(new Function<byte[], Tuple2<Track, byte[]>>() {

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
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = 
						loggerSingleton.getSupplier(new JavaSparkContext(trackRDD.context()));

				trackRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
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
						byte[] compressedResBytes = ByteArrayFactory.compress(resBytes);
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
								if (verbose) {
									loggerSupplier.get().info("PedestrianAttrRecogApp: Sending to Kafka: <" +
											topic + ">" + restExecQueue + "=" + "An attr");
								}
								producerSupplier.get().send(
										new ProducerRecord<String, byte[]>(
												topic,
												restExecQueue,
												compressedDataQueue));
							}
						}
						
						//Always send to the meta data saving application.
						if (verbose) {
							loggerSupplier.get().info("PedestrianAttrRecogApp: Sending to Kafka: <" +
									MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC + ">" + "An attr");
						}
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

	/* (non-Javadoc)
	 * @see org.casia.cripac.isee.vpe.common.SparkStreamingApp#getAppName()
	 */
	@Override
	public String getAppName() {
		return APP_NAME;
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
