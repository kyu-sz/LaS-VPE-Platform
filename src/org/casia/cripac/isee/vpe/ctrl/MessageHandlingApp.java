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
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianReIDUsingAttrApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.BroadcastSingleton;
import org.casia.cripac.isee.vpe.common.KafkaProducerFactory;
import org.casia.cripac.isee.vpe.common.ObjectSupplier;
import org.casia.cripac.isee.vpe.common.SerializationHelper;
import org.casia.cripac.isee.vpe.common.SparkStreamingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.casia.cripac.isee.vpe.ctrl.TaskData.MutableExecutionPlan;
import org.casia.cripac.isee.vpe.data.DataManagingApp;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.casia.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

import scala.Tuple2;

/**
 * The MessageHandlingApp class is a Spark Streaming application responsible for
 * receiving commands from sources like web-UI, then producing appropriate
 * command messages and sending to command-defined starting application.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class MessageHandlingApp extends SparkStreamingApp {

	/**
	 * This class stores possible commands and the String expressions of them.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public static class CommandSet {
		public final static String TRACK_ONLY = "track-only";
		public final static String TRACK_AND_RECOG_ATTR = "track-and-recog-attr";
		public final static String RECOG_ATTR_ONLY = "recog-attr-only";
		public final static String REID_ONLY = "reid-only";
		public final static String RECOG_ATTR_AND_REID = "recog-attr-and-reid";
		public final static String TRACK_AND_RECOG_ATTR_AND_REID = "track-and-recog-attr-and-reid";
	}

	private static final long serialVersionUID = -942388332211825622L;

	/**
	 * The name of this application.
	 */
	public static final String APP_NAME = "MessageHandling";
	public static final String COMMAND_TOPIC = "command";

	/**
	 * Register these topics to the TopicManager, so that on the start of the
	 * module, the TopicManager can help register the topics this application
	 * needs to Kafka brokers.
	 */
	static {
		TopicManager.registerTopic(COMMAND_TOPIC);
	}

	private Map<String, Integer> cmdTopicMap = new HashMap<>();
	private Properties producerProperties;
	private transient SparkConf sparkConf;
	private Map<String, String> kafkaParams;
	private boolean verbose = false;
	private String messageListenerAddr;
	private int messageListenerPort;
	private int numRecvStreams;

	/**
	 * The constructor method. It sets the configurations, but does not start
	 * the contexts.
	 * 
	 * @param propertyCenter
	 *            The propertyCenter stores all the available configurations.
	 */
	public MessageHandlingApp(SystemPropertyCenter propertyCenter) {

		super();

		verbose = propertyCenter.verbose;

		messageListenerAddr = propertyCenter.reportListenerAddress;
		messageListenerPort = propertyCenter.reportListenerPort;

		numRecvStreams = propertyCenter.numRecvStreams;

		cmdTopicMap.put(COMMAND_TOPIC, propertyCenter.kafkaPartitions);

		producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", propertyCenter.kafkaBrokers);
		producerProperties.put("compression.codec", "1");
		producerProperties.put("max.request.size", "10000000");
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		// Create contexts.
		sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.rdd.compress", "true")
				.set("spark.streaming.receiver.writeAheadLog.enable", "true")
				.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
				.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");

		if (!propertyCenter.onYARN) {
			sparkConf = sparkConf.setMaster(propertyCenter.sparkMaster).set("deploy.mode",
					propertyCenter.sparkDeployMode);
		}

		kafkaParams = new HashMap<>();
		System.out.println("|INFO|MessageHandlingApp: metadata.broker.list=" + propertyCenter.kafkaBrokers);
		kafkaParams.put("zookeeper.connect", propertyCenter.zookeeperConnect);
		kafkaParams.put("metadata.broker.list", propertyCenter.kafkaBrokers);
		kafkaParams.put("group.id", "MessageHandlingApp" + UUID.randomUUID());
		// Determine where the stream starts (default: largest)
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("fetch.message.max.bytes", "" + propertyCenter.kafkaFetchMessageMaxBytes);
	}

	public static class UnsupportedCommandException extends Exception {
		private static final long serialVersionUID = -940732652485656739L;
	}

	private ExecutionPlan createPlanByCommand(String cmd, Serializable data)
			throws UnsupportedCommandException, ClassNotFoundException, IOException {
		MutableExecutionPlan plan = null;

		switch (cmd) {
		case CommandSet.TRACK_ONLY:
			plan = new MutableExecutionPlan();
			// Do tracking only.
			plan.addNode(PedestrianTrackingApp.VIDEO_URL_TOPIC);
			break;
		case CommandSet.RECOG_ATTR_ONLY: {
			plan = new MutableExecutionPlan();
			// Retrieve track data, then feed it to attr recog module.
			int trackDataNodeID = plan.addNode(DataManagingApp.PEDESTRIAN_TRACK_RTRV_JOB_TOPIC);
			int attrRecogNodeID = plan.addNode(PedestrianAttrRecogApp.TRACK_TOPIC);
			plan.linkNodes(trackDataNodeID, attrRecogNodeID);
			break;
		}
		case CommandSet.TRACK_AND_RECOG_ATTR: {
			plan = new MutableExecutionPlan();
			// Do tracking, then output to attr recog module.
			int trackingNodeID = plan.addNode(PedestrianTrackingApp.VIDEO_URL_TOPIC);
			int attrRecogNodeID = plan.addNode(PedestrianAttrRecogApp.TRACK_TOPIC);
			plan.linkNodes(trackingNodeID, attrRecogNodeID);
			break;
		}
		case CommandSet.REID_ONLY: {
			plan = new MutableExecutionPlan();
			// Retrieve track and attr data integrally, then feed them to ReID
			// module.
			int trackWithAttrDataNodeID = plan.addNode(DataManagingApp.PEDESTRIAN_TRACK_WITH_ATTR_RTRV_JOB_TOPIC);
			int reidNodeID = plan.addNode(PedestrianReIDUsingAttrApp.TRACK_WTH_ATTR_TOPIC);
			plan.linkNodes(trackWithAttrDataNodeID, reidNodeID);
			break;
		}
		case CommandSet.RECOG_ATTR_AND_REID: {
			plan = new MutableExecutionPlan();
			int trackDataNodeID = plan.addNode(DataManagingApp.PEDESTRIAN_TRACK_RTRV_JOB_TOPIC);
			int attrRecogNodeID = plan.addNode(PedestrianAttrRecogApp.TRACK_TOPIC);
			int reidTrackNodeID = plan.addNode(PedestrianReIDUsingAttrApp.TRACK_TOPIC);
			int reidAttrNodeID = plan.addNode(PedestrianReIDUsingAttrApp.ATTR_TOPIC);
			plan.linkNodes(trackDataNodeID, attrRecogNodeID);
			plan.linkNodes(trackDataNodeID, reidTrackNodeID);
			plan.linkNodes(attrRecogNodeID, reidAttrNodeID);
			break;
		}
		case CommandSet.TRACK_AND_RECOG_ATTR_AND_REID: {
			plan = new MutableExecutionPlan();
			int trackingNodeID = plan.addNode(PedestrianTrackingApp.VIDEO_URL_TOPIC);
			int attrRecogNodeID = plan.addNode(PedestrianAttrRecogApp.TRACK_TOPIC);
			int reidTrackNodeID = plan.addNode(PedestrianReIDUsingAttrApp.TRACK_TOPIC);
			int reidAttrNodeID = plan.addNode(PedestrianReIDUsingAttrApp.ATTR_TOPIC);
			plan.linkNodes(trackingNodeID, attrRecogNodeID);
			plan.linkNodes(trackingNodeID, reidTrackNodeID);
			plan.linkNodes(attrRecogNodeID, reidAttrNodeID);
			break;
		}
		default:
			throw new UnsupportedCommandException();
		}

		return plan.createImmutableCopy();
	}

	@Override
	protected JavaStreamingContext getStreamContext() {
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));

		// Create KafkaSink for Spark Streaming to output to Kafka.
		final BroadcastSingleton<KafkaProducer<String, byte[]>> producerSingleton = new BroadcastSingleton<KafkaProducer<String, byte[]>>(
				new KafkaProducerFactory<>(producerProperties), KafkaProducer.class);

		final BroadcastSingleton<SynthesizedLogger> loggerSingleton = new BroadcastSingleton<>(
				new SynthesizedLoggerFactory(messageListenerAddr, messageListenerPort), SynthesizedLogger.class);

		JavaPairDStream<String, byte[]> cmdStream = buildBytesDirectInputStream(streamingContext, numRecvStreams,
				kafkaParams, cmdTopicMap);

		// Handle the messages received from Kafka,
		cmdStream.foreachRDD(new VoidFunction<JavaPairRDD<String, byte[]>>() {
			private static final long serialVersionUID = 5448084941313023969L;

			@Override
			public void call(JavaPairRDD<String, byte[]> rdd) throws Exception {

				final ObjectSupplier<KafkaProducer<String, byte[]>> producerSupplier = producerSingleton
						.getSupplier(new JavaSparkContext(rdd.context()));
				final ObjectSupplier<SynthesizedLogger> loggerSupplier = loggerSingleton
						.getSupplier(new JavaSparkContext(rdd.context()));

				rdd.context().setLocalProperty("spark.scheduler.pool", "vpe");

				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, byte[]>>>() {

					private static final long serialVersionUID = -4594138896649250346L;

					@Override
					public void call(Iterator<Tuple2<String, byte[]>> msgIter) throws Exception {

						while (msgIter.hasNext()) {
							// Get a next command message.
							Tuple2<String, byte[]> msg = msgIter.next();

							UUID taskID = UUID.randomUUID();

							String cmd = msg._1();
							Serializable data = SerializationHelper.deserialize(msg._2());

							ExecutionPlan plan = createPlanByCommand(cmd, data);

							Integer[] startableNodes = plan.getStartableNodes();
							for (int nodeID : startableNodes) {
								TaskData taskData = new TaskData(nodeID, plan, data);
								Future<RecordMetadata> future = producerSupplier.get()
										.send(new ProducerRecord<String, byte[]>(plan.getNode(nodeID).getTopic(),
												taskID.toString(), SerializationHelper.serialize(taskData)));
								RecordMetadata metadata = future.get();
								if (verbose) {
									loggerSupplier.get()
											.info(APP_NAME + ": Sent initial task for command " + cmd + " to <"
													+ metadata.topic() + "-" + metadata.partition() + "-"
													+ metadata.offset() + ">: Task " + taskID.toString() + " with "
													+ taskData.predecessorResult);
								}
							}

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

	public static void main(String[] args) throws URISyntaxException, ParserConfigurationException, SAXException {

		SystemPropertyCenter propertyCenter;
		if (args.length > 0) {
			propertyCenter = new SystemPropertyCenter(args);
		} else {
			propertyCenter = new SystemPropertyCenter();
		}

		TopicManager.checkTopics(propertyCenter);

		MessageHandlingApp messageHandlingApp = new MessageHandlingApp(propertyCenter);
		messageHandlingApp.initialize(propertyCenter);
		messageHandlingApp.start();
		messageHandlingApp.awaitTermination();
	}
}
