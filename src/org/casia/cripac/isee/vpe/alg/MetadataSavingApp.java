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
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class MetadataSavingApp extends SparkStreamingApp {
	
	private static final long serialVersionUID = -4167212422997458537L;
	private static final String APPLICATION_NAME = "MetadataSaving";
	private String sparkMaster;
	private String kafkaBrokers;
	private HashSet<String> topicsSet = new HashSet<>();
	
	public MetadataSavingApp(String sparkMaster, String kafkaBrokers) {
		this.sparkMaster = sparkMaster;
		this.kafkaBrokers = kafkaBrokers;
		
		topicsSet.add(PedestrianTrackingApp.TRACKING_RESULT_TOPIC);
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
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		JavaPairInputDStream<String, String> messagesDStream =
				KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		//Display the messages.
		//TODO Modify the streaming steps from here to store the meta data.
		messagesDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = -1269453288342585510L;

			@Override
			public void call(JavaPairRDD<String, String> resultsRDD) throws Exception {
				resultsRDD.foreach(new VoidFunction<Tuple2<String,String>>() {

					private static final long serialVersionUID = 6465526220612689594L;

					@Override
					public void call(Tuple2<String, String> result) throws Exception {
						System.out.printf("Metadata saver received: <%s>%s\n", result._1(), result._2());
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
