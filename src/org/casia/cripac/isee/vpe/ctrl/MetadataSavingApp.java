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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.ByteArrayFactory;
import org.casia.cripac.isee.vpe.common.HadoopUtils;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
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
	private HashSet<String> pedestrianTrackTopicsSet = new HashSet<>();
	private HashSet<String> pedestrianAttrTopicsSet = new HashSet<>();
	private transient SparkConf sparkConf;
	private Map<String, String> commonKafkaParams = new HashMap<>();
	private String metadataSavingDir;
	private String trackSavingDir;

	public static final String APPLICATION_NAME = "MetadataSaving";
	public static final String PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC = "pedestrian-track-saving-input";
	public static final String PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC = "pedestrian-attr-saving-input";
	
	public MetadataSavingApp(SystemPropertyCenter propertyCenter) throws IOException, IllegalArgumentException, ParserConfigurationException, SAXException {
		pedestrianTrackTopicsSet.add(PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC);
		pedestrianAttrTopicsSet.add(PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC);

		//Create contexts.
		sparkConf = new SparkConf()
				.setAppName(APPLICATION_NAME);
		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf
					.setMaster(propertyCenter.sparkMaster)
					.set("deploy.mode", propertyCenter.sparkDeployMode);
		}
		
		//Common Kafka settings
		commonKafkaParams.put("group.id", "0");
		commonKafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);

//		metadataSavingDir = "hdfs://" + propertyCenter.hdfs + "/metadata";
		metadataSavingDir = "/metadata";
		
		trackSavingDir = metadataSavingDir + "/track";
		FileSystem.get(HadoopUtils.getDefaultConf()).mkdirs(new Path(trackSavingDir));
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLogLevel("WARN");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		BroadcastSingleton<FileSystem> fileSystemSingleton =
				new BroadcastSingleton<>(new ObjectFactory<FileSystem>() {

					private static final long serialVersionUID = 1L;

					@Override
					public FileSystem getObject() {
						Configuration hdfsConf;
						try {
							hdfsConf = HadoopUtils.getDefaultConf();
							hdfsConf.setBoolean("dfs.support.append", true);
							return FileSystem.get(hdfsConf);
						} catch (ParserConfigurationException | SAXException | IOException e) {
							e.printStackTrace();
							return null;
						}
					}
				}, FileSystem.class); 
		
		//Retrieve tracks from Kafka.
		JavaPairInputDStream<String, byte[]> trackByteArrayDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, pedestrianTrackTopicsSet);
		
		//Extract videoURLs from the tracks to use as keys.
		JavaPairDStream<String,Track> trackDStream =
		trackByteArrayDStream.mapToPair(new PairFunction<Tuple2<String,byte[]>, String, Track>() {
			private static final long serialVersionUID = -4573981130172486130L;

			@Override
			public Tuple2<String, Track> call(Tuple2<String, byte[]> result) throws Exception {
				Track track = (Track) ByteArrayFactory.getObject(
						ByteArrayFactory.decompress(result._2()));
				return new Tuple2<String, Track>(track.videoURL, track);
			}
		});
		
		//Group the tracks by the videoURLs.
		JavaPairDStream<String, Iterable<Track>> trackGroupDStream = trackDStream.groupByKey();

		//Save the track groups to an HDFS file.
		trackGroupDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Iterable<Track>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Iterable<Track>> trackGroupRDD) throws Exception {
				
				final ObjectSupplier<FileSystem> fsSupplier = fileSystemSingleton.getSupplier(
						new JavaSparkContext(trackGroupRDD.context()));
				
				trackGroupRDD.foreach(new VoidFunction<Tuple2<String,Iterable<Track>>>() {

					private static final long serialVersionUID = -7729434941232380812L;

					@Override
					public void call(Tuple2<String, Iterable<Track>> trackGroup) throws Exception {
						String videoURL = trackGroup._1();
						String dst = trackSavingDir + "/" + videoURL;
						
						FSDataOutputStream outputStream = fsSupplier.get().append(new Path(dst));
						
						//TODO Convert the track group into string.
						byte[] bytes = trackGroup.toString().getBytes();
						outputStream.write(bytes, 0, bytes.length);
						
						outputStream.close();
					}
				});
			}
		});

		//Retrieve attributes from Kafka
		JavaPairInputDStream<String, byte[]> attrDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, commonKafkaParams, pedestrianAttrTopicsSet);
		
		//Display the attributes.
		//TODO Modify the streaming steps from here to store the meta data.
		attrDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -1269453288342585510L;

			@Override
			public void call(JavaPairRDD<String, byte[]> resultsRDD) throws Exception {
				resultsRDD.foreach(new VoidFunction<Tuple2<String,byte[]>>() {

					private static final long serialVersionUID = 6465526220612689594L;

					@Override
					public void call(Tuple2<String, byte[]> result) throws Exception {
						Attribute attr = (Attribute) ByteArrayFactory.getObject(
								ByteArrayFactory.decompress(result._2()));
						
						System.out.printf("Metadata saver received attribute: %s=%s\n",
								result._1(),
								attr.facing);
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

		TopicManager.checkTopics(propertyCenter);
		
		MetadataSavingApp metadataSavingApp = new MetadataSavingApp(propertyCenter);
		metadataSavingApp.initialize(propertyCenter);
		metadataSavingApp.start();
		metadataSavingApp.awaitTermination();
	}
}
