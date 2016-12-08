/***********************************************************************
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.cripac.isee.vpe.alg;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.attr.ExternPedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.common.Topic;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.cripac.isee.vpe.util.Factory;
import org.cripac.isee.vpe.util.SerializationHelper;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.*;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The PedestrianAttrRecogApp class is a Spark Streaming application which
 * performs pedestrian attribute recognition.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianAttrRecogApp extends SparkStreamingApp {
    /**
     * The NAME of this application.
     */
    public static final String APP_NAME = "PedestrianAttrRecognizing";

    private Stream attrRecogStream;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianAttrRecogApp(SystemPropertyCenter propCenter) throws Exception {
        attrRecogStream = new RecogStream(propCenter);
    }

    /**
     * @param args No options supported currently.
     * @throws Exception Any exception that might occur during execution.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        SystemPropertyCenter propCenter;
        propCenter = new SystemPropertyCenter(args);

        // Start the pedestrian tracking application.
        PedestrianAttrRecogApp app = new PedestrianAttrRecogApp(propCenter);
        TopicManager.checkTopics(propCenter);
        app.initialize(propCenter);
        app.start();
        app.awaitTermination();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * SparkStreamingApp#getStreamContext()
     */
    @Override
    protected JavaStreamingContext getStreamContext() {
        // Create contexts.
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf(true));
        sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(2));

        attrRecogStream.addToContext(jsc);

        return jsc;
    }

    /*
     * (non-Javadoc)
     *
     * @see SparkStreamingApp#getStreamInfo()
     */
    @Override
    public String getAppName() {
        return APP_NAME;
    }

    public static class RecogStream extends Stream {

        public static final Info INFO = new Info("RecogStream", DataType.ATTR);

        /**
         * Topic to input tracklets from Kafka.
         */
        public static final Topic TRACKLET_TOPIC = new Topic(
                "pedestrian-tracklet-for-attr-recog", DataType.TRACKLET, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages from Kafka
         * Brokers.
         */
        private Map<String, String> kafkaParams = new HashMap<>();
        /**
         * Topics for inputting tracklets. Each assigned a number of threads the Kafka
         * consumer should use.
         */
        private Map<String, Integer> trackletTopicMap = new HashMap<>();

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<PedestrianAttrRecognizer> attrRecogSingleton;
        private Singleton<SynthesizedLogger> loggerSingleton;

        public RecogStream(SystemPropertyCenter propCenter) throws Exception {
            trackletTopicMap.put(TRACKLET_TOPIC.NAME, propCenter.kafkaNumPartitions);

            // Common kafka settings.
            kafkaParams.put("group.id", "PedestrianAttrRecogApp" + UUID.randomUUID());
            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            // Determine where the stream starts (default: largest)
            kafkaParams.put("auto.offset.reset", "smallest");
            kafkaParams.put("metadata.broker.list", propCenter.kafkaBrokers);
            kafkaParams.put("fetch.message.max.bytes", "" + propCenter.kafkaFetchMsgMaxBytes);

            Properties producerProp = new Properties();
            producerProp.put("bootstrap.servers", propCenter.kafkaBrokers);
            producerProp.put("compression.codec", "1");
            producerProp.put("max.request.size", "10000000");
            producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProp.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));
            attrRecogSingleton = new Singleton<>(() -> new ExternPedestrianAttrRecognizer(
                    Inet4Address.getByName("192.168.1.90"), 8500
            ));
            loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(
                    INFO.NAME,
                    propCenter.verbose ? Level.DEBUG : Level.INFO,
                    propCenter.reportListenerAddr,
                    propCenter.reportListenerPort));
        }

        @Override
        public void addToContext(JavaStreamingContext jsc) {// Extract tracklets from the data.
            // Recognize attributes from the tracklets.
            buildBytesDirectStream(jsc, kafkaParams, trackletTopicMap)
                    .mapValues(taskDataBytes ->
                            (TaskData) deserialize(taskDataBytes))
                    .foreachRDD(rdd -> {
                        rdd.foreach(taskWithTracklet -> {
                            Logger logger = loggerSingleton.getInst();

                            String taskID = taskWithTracklet._1();
                            TaskData taskData = taskWithTracklet._2();

                            if (!(taskData.predecessorRes instanceof Tracklet)) {
                                logger.fatal("Predecessor result sent by "
                                                + taskData.predecessorInfo
                                                + " is expected to be a tracklet,"
                                                + " but received \""
                                                + taskData.predecessorRes + "\"!");
                                return;
                            }

                            Tracklet tracklet = (Tracklet) taskData.predecessorRes;

                            // Recognize attributes.
                            Attributes attr = attrRecogSingleton.getInst().recognize(tracklet);
                            attr.trackletID = tracklet.id;

                            // Prepare new task data.
                            // Stored the track in the task data, which can be
                            // cyclic utilized.
                            taskData.predecessorRes = attr;
                            // Get the IDs of successor nodes.
                            List<Topic> succTopics = taskData.curNode.getSuccessors();
                            // Mark the current node as executed.
                            taskData.curNode.markExecuted();
                            // Send to all the successor nodes.
                            for (Topic topic : succTopics) {
                                taskData.changeCurNode(topic);
                                sendWithLog(topic,
                                        taskID,
                                        serialize(taskData),
                                        producerSingleton.getInst(),
                                        logger);
                            }
                        });
                    });
        }
    }
}
