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

package org.casia.cripac.isee.vpe.ctrl;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.pedestrian.tracking.Track.BoundingBox;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SynthesizedLogger;
import org.casia.cripac.isee.vpe.common.SynthesizedLoggerFactory;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.xml.sax.SAXException;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * <br>This class saves meta data to HDFS and Neo4j database.</br>
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class MetadataSavingApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = -4167212422997458537L;
//	private HashSet<String> pedestrianTrackTopicsSet = new HashSet<>();
//	private HashSet<String> pedestrianAttrTopicsSet = new HashSet<>();
	private Map<String, Integer> trackTopicPartitions = new HashMap<>();
	private Map<String, Integer> attrTopicPartitions = new HashMap<>();
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private String metadataSavingDir;
	private String trackSavingDir;
	private boolean verbose = false;
	private String messageListenerAddr;
	private int messageListenerPort;

	public static final String APPLICATION_NAME = "MetadataSaving";
	public static final String PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC = "pedestrian-track-saving-input";
	public static final String PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC = "pedestrian-attr-saving-input";
	
	public MetadataSavingApp(SystemPropertyCenter propertyCenter) throws IOException, IllegalArgumentException, ParserConfigurationException, SAXException {
		
		verbose = propertyCenter.verbose;
		
		messageListenerAddr = propertyCenter.messageListenerAddress;
		messageListenerPort = propertyCenter.messageListenerPort;
		
		trackTopicPartitions.put(PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC, propertyCenter.kafkaPartitions);
		attrTopicPartitions.put(PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC, propertyCenter.kafkaPartitions);
//		pedestrianTrackTopicsSet.add(PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC);
//		pedestrianAttrTopicsSet.add(PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC);
		
		SynthesizedLogger logger = new SynthesizedLogger(messageListenerAddr, messageListenerPort);
		logger.info("MessageHandlingApp: spark.dynamic.allocation.enabled="
				+ propertyCenter.sparkDynamicAllocationEnabled);
		logger.info("MessageHandlingApp: spark.streaming.dynamic.allocation.enabled="
				+ propertyCenter.sparkStreamingDynamicAllocationEnabled);
		logger.info("MessageHandlingApp: spark.streaming.dynamicAllocation.minExecutors="
				+ propertyCenter.sparkStreamingDynamicAllocationMinExecutors);
		logger.info("MessageHandlingApp: spark.streaming.dynamicAllocation.maxExecutors="
				+ propertyCenter.sparkStreamingDynamicAllocationMaxExecutors);

		//Create contexts.
		sparkConf = new SparkConf()
				.setAppName(APPLICATION_NAME)
				.set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");
		
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}
		
		//Common Kafka settings
		commonKafkaParams.put("group.id", "MetadataSavingApp" + UUID.randomUUID());
		commonKafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		// Determine where the stream starts (default: largest)
		commonKafkaParams.put("auto.offset.reset", "smallest");
		commonKafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);

//		metadataSavingDir = "hdfs://" + propertyCenter.hdfs + "/metadata";
		metadataSavingDir = "/metadata";
		
		trackSavingDir = metadataSavingDir + "/track";
		FileSystem.get(new Configuration()).mkdirs(new Path(trackSavingDir));
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		final BroadcastSingleton<FileSystem> fileSystemSingleton =
				new BroadcastSingleton<>(new ObjectFactory<FileSystem>() {

					private static final long serialVersionUID = 300022787313821456L;

					@Override
					public FileSystem getObject() {
						Configuration hdfsConf;
						try {
							hdfsConf = new Configuration();
							hdfsConf.setBoolean("dfs.support.append", true);
							return FileSystem.get(hdfsConf);
						} catch (IOException e) {
							e.printStackTrace();
							return null;
						}
					}
				}, FileSystem.class); 

		final BroadcastSingleton<SynthesizedLogger> loggerSingleton =
				new BroadcastSingleton<>(
						new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort),
						SynthesizedLogger.class);

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by Zookeeper.
		 * TODO Create multiple input streams and unite them together for higher receiving speed.
		 * @link http://spark.apache.org/docs/latest/streaming-programming-guide.html#level-of-parallelism-in-data-receiving
		 * TODO Find ways to robustly make use of createDirectStream.
		 */
		JavaPairReceiverInputDStream<String, byte[]> trackBytesDStream = KafkaUtils.createStream(
				streamingContext,
				String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
				commonKafkaParams,
				trackTopicPartitions, 
				StorageLevel.MEMORY_AND_DISK_SER());
//		//Retrieve tracks from Kafka.
//		JavaPairInputDStream<String, byte[]> trackByteArrayDStream =
//				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
//				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, pedestrianTrackTopicsSet);
		
		trackBytesDStream.mapToPair(new PairFunction<Tuple2<String,byte[]>, UUID, Track>() {

			private static final long serialVersionUID = -9140201497081719411L;

			@Override
			public Tuple2<UUID, Track> call(Tuple2<String, byte[]> trackBytes) throws Exception {
				Track track = (Track) ByteArrayFactory.getObject(ByteArrayFactory.decompress(trackBytes._2()));
				return new Tuple2<UUID, Track>(track.taskID, track);
			}
			
		}).groupByKey().foreachRDD(new VoidFunction<JavaPairRDD<UUID, Iterable<Track>>>() {

			private static final long serialVersionUID = -6731502755371825010L;

			@Override
			public void call(JavaPairRDD<UUID, Iterable<Track>> trackGroupRDD) throws Exception {
				
				final ObjectSupplier<FileSystem> fsSupplier = fileSystemSingleton.getSupplier(
						new JavaSparkContext(trackGroupRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton.getSupplier(
						new JavaSparkContext(trackGroupRDD.context()));
				
				trackGroupRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				trackGroupRDD.foreach(new VoidFunction<Tuple2<UUID,Iterable<Track>>>() {

					private static final long serialVersionUID = 5522067102611597772L;

					@Override
					public void call(Tuple2<UUID, Iterable<Track>> trackGroup) throws Exception {
						
						FileSystem fs = fsSupplier.get();
						
						UUID taskID = trackGroup._1();
						Iterator<Track> trackIterator = trackGroup._2().iterator();
						Track track = trackIterator.next();
						String videoURL = track.videoURL;
						int numTracks = track.numTracks;
						String storeRoot = metadataSavingDir + "/" + videoURL + "/" + taskID.toString();
						fs.mkdirs(new Path(storeRoot));
						
						while (true) {
							String storeDir = storeRoot + "/" + track.id;
							fs.mkdirs(new Path(storeDir));
							
							int numBBoxes = track.locationSequence.size();
							
							// Write bounding boxes infos.
							FSDataOutputStream outputStream = fs.create(new Path(storeDir + "/bbox.txt"));
							BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
							writer.write("{");
							writer.newLine();
							writer.write("\t\"startFrameIndex\":" + track.startFrameIndex);
							writer.newLine();
							writer.write("\t\"boundingBoxes\":[");
							
							Iterator<BoundingBox> bboxIter = track.locationSequence.iterator();
							for (int i = 0; i < numBBoxes; ++i) {
								BoundingBox bbox = bboxIter.next();
								
								writer.write("{");
								writer.newLine();
								writer.write("\t\t\"x\": " + bbox.x + ",");
								writer.newLine();
								writer.write("\t\t\"y\": " + bbox.y + ",");
								writer.newLine();
								writer.write("\t\t\"width\": " + bbox.width + ",");
								writer.newLine();
								writer.write("\t\t\"height\": " + bbox.height);
								writer.newLine();
								writer.write("\t}");
								if (bboxIter.hasNext()) {
									writer.write(", ");
								}
								
								// Use JavaCV to encode the image patch into a JPEG, stored in the memory.
								BytePointer inputPointer = new BytePointer(bbox.patchData); 
								Mat image = new Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
								BytePointer outputPointer = new BytePointer();
								imencode("jpg", image, outputPointer);
								byte[] bytes = new byte[(int) outputPointer.limit()];
								outputPointer.get(bytes);

								// Output the image patch to HDFS.
								FSDataOutputStream imgOutputStream = fs.create(new Path(storeDir + "/" + i + ".jpg"));
								imgOutputStream.write(bytes);
								imgOutputStream.close();
							}
							
							writer.write("\t]");
							writer.newLine();
							writer.write("}");
							writer.newLine();
							writer.flush();
							writer.close();
							outputStream.close();
							
							if (!trackIterator.hasNext()) {
								break;
							}
							track = trackIterator.next();
						}
						
						ContentSummary contentSummary = fs.getContentSummary(new Path(storeRoot));
						long cnt = contentSummary.getDirectoryCount();
						if (cnt == numTracks) {
							loggerSupplier.get().info("Task " + videoURL + "-" + taskID + " finished!");
							
							HarFileSystem harFileSystem = new HarFileSystem(fs);
							// TODO Pack all the results of a task into a HAR.
							harFileSystem.copyFromLocalFile(true, new Path(storeRoot), new Path(storeRoot + ".har"));
							harFileSystem.close();
							
							loggerSupplier.get().info("Tracks of " + videoURL + "-" + taskID + " packed!");
						}
					}
				});
			}
		});

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by Zookeeper.
		 * TODO Create multiple input streams and unite them together for higher receiving speed.
		 * @link http://spark.apache.org/docs/latest/streaming-programming-guide.html#level-of-parallelism-in-data-receiving
		 * TODO Find ways to robustly make use of createDirectStream.
		 */
		JavaPairReceiverInputDStream<String, byte[]> attrDStream = KafkaUtils.createStream(
				streamingContext,
				String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
				commonKafkaParams,
				attrTopicPartitions, 
				StorageLevel.MEMORY_AND_DISK_SER());
//		//Retrieve attributes from Kafka
//		JavaPairInputDStream<String, byte[]> attrDStream =
//				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
//				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, pedestrianAttrTopicsSet);
		
		//Display the attributes.
		//TODO Modify the streaming steps from here to store the meta data.
		attrDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -715024705240889905L;

			@Override
			public void call(JavaPairRDD<String, byte[]> attrRDD) throws Exception {
				
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton.getSupplier(
						new JavaSparkContext(attrRDD.context()));
				
//				System.out.println("RDD count: " + attrRDD.count() + " partitions:" + attrRDD.getNumPartitions()
//				+ " " + attrRDD.toString());

				attrRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				attrRDD.foreach(new VoidFunction<Tuple2<String,byte[]>>() {

					private static final long serialVersionUID = -4846631314801254257L;

					@Override
					public void call(Tuple2<String, byte[]> result) throws Exception {
						Attribute attr;
						try {
							attr = (Attribute) ByteArrayFactory.getObject(
									ByteArrayFactory.decompress(result._2()));

							if (verbose) {
								loggerSupplier.get().info("Metadata saver received attribute: "
										+ "Facing" + "=" + attr.facing + "; Sex=" + attr.sex);
							}
						} catch (IOException e) {
							loggerSupplier.get().error("Exception caught when decompressing attributes", e);
						}
					}
					
				});
			}
		});
		
		return streamingContext;
	}

	public static void main(String[] args) throws IOException, URISyntaxException, ParserConfigurationException, SAXException {
	
		SystemPropertyCenter propertyCenter;
		if (args.length > 0) {
			propertyCenter = new SystemPropertyCenter(args);
		} else {
			propertyCenter = new SystemPropertyCenter();
		}
		
		if (propertyCenter.verbose) {
			System.out.println("Starting MetadataSavingApp...");
		}

		TopicManager.checkTopics(propertyCenter);
		
		MetadataSavingApp metadataSavingApp = new MetadataSavingApp(propertyCenter);
		metadataSavingApp.initialize(propertyCenter);
		metadataSavingApp.start();
		metadataSavingApp.awaitTermination();
	}
}
