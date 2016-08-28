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

package org.casia.cripac.isee.vpe.data;

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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.helper.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_imgproc;
import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.pedestrian.tracking.Track.BoundingBox;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.TopicManager;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

import scala.Tuple2;

/**
 * <br>
 * This class saves meta data to HDFS and Neo4j database.</br>
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class MetadataSavingApp extends SparkStreamingApp {

	private static final long serialVersionUID = -4167212422997458537L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "MetadataSaving";
	public static final String PEDESTRIAN_TRACK_TOPIC = "pedestrian-track-for-saving";
	public static final String PEDESTRIAN_ATTR_TOPIC = "pedestrian-attr-for-saving";
	public static final String PEDESTRIAN_ID_TOPIC = "pedestrian-id-for-saving";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * whole system, the TopicManager can help register the topics this
	 * application needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(PEDESTRIAN_TRACK_TOPIC);
		TopicManager.registerTopic(PEDESTRIAN_ATTR_TOPIC);
		TopicManager.registerTopic(PEDESTRIAN_ID_TOPIC);
	}

	// private HashSet<String> pedestrianTrackTopicsSet = new HashSet<>();
	// private HashSet<String> pedestrianAttrTopicsSet = new HashSet<>();
	private Map<String, Integer> trackTopicMap = new HashMap<>();
	private Map<String, Integer> attrTopicMap = new HashMap<>();
	private Map<String, Integer> idTopicMap = new HashMap<>();
	private transient SparkConf sparkConf;
	private Map<String, String> kafkaParams = new HashMap<>();
	private String metadataDir;
	private boolean verbose = false;
	private String reportListenerAddr;
	private int reportListenerPort;
	private int numRecvStreams;

	public MetadataSavingApp(SystemPropertyCenter propertyCenter)
			throws IOException, IllegalArgumentException, ParserConfigurationException, SAXException {

		verbose = propertyCenter.verbose;

		reportListenerAddr = propertyCenter.reportListenerAddress;
		reportListenerPort = propertyCenter.reportListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		trackTopicMap.put(PEDESTRIAN_TRACK_TOPIC, propertyCenter.kafkaPartitions);
		attrTopicMap.put(PEDESTRIAN_ATTR_TOPIC, propertyCenter.kafkaPartitions);
		idTopicMap.put(PEDESTRIAN_ID_TOPIC, propertyCenter.kafkaPartitions);
		// pedestrianTrackTopicsSet.add(PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC);
		// pedestrianAttrTopicsSet.add(PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC);

		// Create contexts.
		sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");

		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf.setMaster(propertyCenter.sparkMaster).set("deploy.mode",
					propertyCenter.sparkDeployMode);
		}

		// Common Kafka settings
		kafkaParams.put("group.id", "MetadataSavingApp" + UUID.randomUUID());
		kafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		// Determine where the stream starts (default: largest)
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);

		metadataDir = propertyCenter.metadataDir;
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		// Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		final BroadcastSingleton<FileSystem> fileSystemSingleton = new BroadcastSingleton<>(
				new ObjectFactory<FileSystem>() {

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

		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(reportListenerAddr, reportListenerPort), SynthesizedLogger.class);

		JavaPairDStream<String, byte[]> trackBytesStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, trackTopicMap);

		trackBytesStream.groupByKey().foreachRDD(new VoidFunction<JavaPairRDD<String, Iterable<byte[]>>>() {

			private static final long serialVersionUID = -6731502755371825010L;

			@Override
			public void call(JavaPairRDD<String, Iterable<byte[]>> trackGroupRDD) throws Exception {

				final ObjectSupplier<FileSystem> fsSupplier = fileSystemSingleton
						.getSupplier(new JavaSparkContext(trackGroupRDD.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(trackGroupRDD.context()));

				if (verbose) {
					System.out.println(APP_NAME + ": Reading trackGroupRDD...");
					System.out.println(APP_NAME + ": RDD count=" + trackGroupRDD.count() + " partitions="
							+ trackGroupRDD.getNumPartitions() + " " + trackGroupRDD.toString());
					System.out.println(APP_NAME + ": Starting trackGroupRDD.foreach...");
				}
				trackGroupRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				trackGroupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<byte[]>>>() {

					private static final long serialVersionUID = 5522067102611597772L;

					@Override
					public void call(Tuple2<String, Iterable<byte[]>> trackGroup) throws Exception {

						// RuntimeException: No native JavaCPP library in
						// memory. (Has Loader.load() been called?)
						Loader.load(opencv_core.class);
						Loader.load(opencv_imgproc.class);

						FileSystem fs = fsSupplier.get();

						String taskID = trackGroup._1();
						Iterator<byte[]> trackIterator = trackGroup._2().iterator();
						Track track = (Track) SerializationHelper.deserialize(trackIterator.next());
						String videoURL = new String(track.videoURL);
						int numTracks = track.numTracks;
						String storeRoot = metadataDir + "/" + videoURL + "/" + taskID;
						fs.mkdirs(new Path(storeRoot));

						while (true) {
							String storeDir = storeRoot + "/" + track.id;
							fs.mkdirs(new Path(storeDir));

							int numBBoxes = track.locationSequence.length;

							// Write bounding boxes infomations.
							FSDataOutputStream outputStream = fs.create(new Path(storeDir + "/bbox.txt"));
							BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
							writer.write("{");
							writer.newLine();
							writer.write("\t\"startFrameIndex\":" + track.startFrameIndex);
							writer.newLine();
							writer.write("\t\"boundingBoxes\":[");

							for (int i = 0; i < numBBoxes; ++i) {
								BoundingBox bbox = track.locationSequence[i];

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
								if (i + 1 < numBBoxes) {
									writer.write(", ");
								}

								// Use JavaCV to encode the image patch into a
								// JPEG, stored in the memory.
								BytePointer inputPointer = new BytePointer(bbox.patchData);
								Mat image = new Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
								BytePointer outputPointer = new BytePointer();
								imencode(".jpg", image, outputPointer);
								byte[] bytes = new byte[(int) outputPointer.limit()];
								outputPointer.get(bytes);

								// Output the image patch to HDFS.
								FSDataOutputStream imgOutputStream = fs.create(new Path(storeDir + "/" + i + ".jpg"));
								imgOutputStream.write(bytes);
								imgOutputStream.close();

								image.release();
								inputPointer.deallocate();
								outputPointer.deallocate();
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
							track = (Track) SerializationHelper.deserialize(trackIterator.next());
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

						System.gc();
					}
				});
			}
		});

		/**
		 * Though the "createDirectStream" method is suggested for higher speed,
		 * we use createStream for auto management of Kafka offsets by
		 * Zookeeper. TODO Find ways to robustly make use of createDirectStream.
		 */
		JavaPairDStream<String, byte[]> attrStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, attrTopicMap);
		// //Retrieve attributes from Kafka
		// JavaPairInputDStream<String, byte[]> attrDStream =
		// KafkaUtils.createDirectStream(streamingContext, String.class,
		// byte[].class,
		// StringDecoder.class, DefaultDecoder.class, commonKafkaParams,
		// pedestrianAttrTopicsSet);

		// Display the attributes.
		// TODO Modify the streaming steps from here to store the meta data.
		attrStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -715024705240889905L;

			@Override
			public void call(JavaPairRDD<String, byte[]> attrRDD) throws Exception {

				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(attrRDD.context()));

				attrRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				attrRDD.foreach(new VoidFunction<Tuple2<String, byte[]>>() {

					private static final long serialVersionUID = -4846631314801254257L;

					@Override
					public void call(Tuple2<String, byte[]> result) throws Exception {
						Attributes attr;
						try {
							attr = (Attributes) SerializationHelper.deserialize(result._2());

							if (verbose) {
								loggerSupplier.get().info("Metadata saver received " + result._1() + ": " + attr);
							}
						} catch (IOException e) {
							loggerSupplier.get().error("Exception caught when decompressing attributes", e);
						}
					}

				});
			}
		});

		JavaPairDStream<String, byte[]> idStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, idTopicMap);
		idStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -715024705240889905L;

			@Override
			public void call(JavaPairRDD<String, byte[]> attrRDD) throws Exception {

				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(attrRDD.context()));

				attrRDD.context().setLocalProperty("spark.scheduler.pool", "vpe");
				attrRDD.foreach(new VoidFunction<Tuple2<String, byte[]>>() {

					private static final long serialVersionUID = -4846631314801254257L;

					@Override
					public void call(Tuple2<String, byte[]> result) throws Exception {
						int id;
						try {
							id = (Integer) SerializationHelper.deserialize(result._2());

							if (verbose) {
								loggerSupplier.get()
										.info("Metadata saver received: " + result._1() + ": Pedestrian." + id);
							}
						} catch (IOException e) {
							loggerSupplier.get().error("Exception caught when decompressing ID", e);
						}
					}

				});
			}
		});

		return streamingContext;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.common.SparkStreamingApp#getAppName()
	 */
	@Override
	public String getAppName() {
		return APP_NAME;
	}

	public static void main(String[] args)
			throws IOException, URISyntaxException, ParserConfigurationException, SAXException {

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