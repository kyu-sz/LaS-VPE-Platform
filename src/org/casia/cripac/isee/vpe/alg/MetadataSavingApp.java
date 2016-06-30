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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.vpe.common.ObjectFactory;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class MetadataSavingApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = -4167212422997458537L;
	private static final String APPLICATION_NAME = "MetadataSaving";
	private String sparkMaster;
	private String kafkaBrokers;
	private HashSet<String> pedestrianTrackTopicsSet = new HashSet<>();
	private HashSet<String> pedestrianAttrTopicsSet = new HashSet<>();
	
	public MetadataSavingApp(String sparkMaster, String kafkaBrokers) {
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		
		pedestrianTrackTopicsSet.add(PedestrianTrackingApp.PEDESTRIAN_TRACK_TOPIC);
		pedestrianAttrTopicsSet.add(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_TOPIC);
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		//Create contexts.
		SparkConf sparkConf = new SparkConf()
				.setMaster(sparkMaster)
				.setAppName(APPLICATION_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
		
		//Retrieve messages from Kafka.
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("group.id", "0");
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		JavaPairInputDStream<String, byte[]> trackDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, pedestrianTrackTopicsSet);
		JavaPairInputDStream<String, byte[]> attrDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, byte[].class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams, pedestrianAttrTopicsSet);
		
		//Display the tracks.
		//TODO Modify the streaming steps from here to store the meta data.
		trackDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {

			private static final long serialVersionUID = -1269453288342585510L;

			@Override
			public void call(JavaPairRDD<String, byte[]> resultsRDD) throws Exception {
				resultsRDD.foreach(new VoidFunction<Tuple2<String,byte[]>>() {

					private static final long serialVersionUID = 6465526220612689594L;

					@Override
					public void call(Tuple2<String, byte[]> result) throws Exception {
						Track track = (Track) ObjectFactory.getObject(result._2());
						
						System.out.printf("Metadata saver received track: %s=%s\n",
								result._1(),
								track.startFrameIndex + " " + track.locationSequence.size());
					}
					
				});
			}
		});
		
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
						Attribute attr = (Attribute) ObjectFactory.getObject(result._2());
						
						System.out.printf("Metadata saver received attribute: %s=%s\n",
								result._1(),
								attr.facing);
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
		
		SystemPropertyCenter propertyCenter;
		try {
			propertyCenter = new SystemPropertyCenter("system.properties");
		} catch (IOException e) {
			e.printStackTrace();
			propertyCenter = new SystemPropertyCenter();
		}
		
		MetadataSavingApp metadataSavingApp =
				new MetadataSavingApp(propertyCenter.sparkMaster, propertyCenter.kafkaBrokers);
		metadataSavingApp.initialize(propertyCenter.checkpointDir);
		metadataSavingApp.start();
		metadataSavingApp.awaitTermination();
	}
}
