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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.attr.ExternPedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
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
    public static final String APP_NAME = "pedestrian-attr-recog";
    private Stream attrRecogStream;
    private int batchDuration = 1000;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianAttrRecogApp(AppPropertyCenter propCenter) throws Exception {
        this.batchDuration = propCenter.batchDuration;
        attrRecogStream = new RecogStream(propCenter);
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;
        public InetAddress externAttrRecogServerAddr = InetAddress.getLocalHost();
        public int externAttrRecogServerPort = 0;

        public AppPropertyCenter(@Nonnull String[] args)
                throws URISyntaxException, ParserConfigurationException, SAXException, UnknownHostException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.ped.attr.ext.ip":
                        externAttrRecogServerAddr = InetAddress.getByName((String) entry.getValue());
                        break;
                    case "vpe.ped.attr.ext.port":
                        externAttrRecogServerPort = new Integer((String) entry.getValue());
                        break;
                }
            }
        }
    }

    /**
     * @param args No options supported currently.
     * @throws Exception Any exception that might occur during execution.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        AppPropertyCenter propCenter = new AppPropertyCenter(args);

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
        JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(batchDuration));

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

        public static final Info INFO = new Info("recog", DataTypes.ATTR);

        /**
         * Topic to input tracklets from Kafka.
         */
        public static final Topic TRACKLET_TOPIC =
                new Topic("pedestrian-tracklet-for-attr-recog",
                        DataTypes.TRACKLET, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages from Kafka
         * Brokers.
         */
        private Map<String, Object> kafkaParams = new HashMap<>();

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<PedestrianAttrRecognizer> attrRecogSingleton;

        private final int procTime;

        public RecogStream(AppPropertyCenter propCenter) throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            this.procTime = propCenter.procTime;

            // Common kafka settings.
            kafkaParams = propCenter.generateKafkaParams(INFO.NAME);

            Properties producerProp = propCenter.generateKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));

            loggerSingleton.getInst().debug("Using Kafka brokers: " + propCenter.kafkaBootstrapServers);

            attrRecogSingleton = new Singleton<>(() -> new ExternPedestrianAttrRecognizer(
                    propCenter.externAttrRecogServerAddr, propCenter.externAttrRecogServerPort,
                    loggerSingleton.getInst()
            ));
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {// Extract tracklets from the data.
            // Recognize attributes from the tracklets.
            buildBytesDirectStream(jssc, Arrays.asList(TRACKLET_TOPIC.NAME), kafkaParams, procTime)
                    .mapValues(taskDataBytes -> {
                        TaskData taskData;
                        try {
                            taskData = (TaskData) deserialize(taskDataBytes);
                            return taskData;
                        } catch (Exception e) {
                            loggerSingleton.getInst().error("During TaskData deserialization", e);
                            return null;
                        }
                    })
                    .foreachRDD(rdd ->
                            rdd.foreach(taskWithTracklet -> {
                                try {
                                    Logger logger = loggerSingleton.getInst();

                                    String taskID = taskWithTracklet._1();
                                    TaskData taskData = taskWithTracklet._2();
                                    logger.debug("Received task " + taskID + "!");

                                    if (taskData.predecessorRes == null) {
                                        throw new DataTypeNotMatchedException("Predecessor result sent by "
                                                + taskData.predecessorInfo
                                                + " is null!");
                                    }
                                    if (!(taskData.predecessorRes instanceof Tracklet)) {
                                        throw new DataTypeNotMatchedException("Predecessor result sent by "
                                                + taskData.predecessorInfo
                                                + " is expected to be a Tracklet,"
                                                + " but received \""
                                                + taskData.predecessorRes + "\"!");
                                    }

                                    Tracklet tracklet = (Tracklet) taskData.predecessorRes;
                                    logger.debug("To recognize attributes for task " + taskID + "!");
                                    // Truncate and shrink the tracklet in case it is too large.
                                    if (tracklet.locationSequence.length > 256) {
                                        int increment = tracklet.locationSequence.length / 256;
                                        int start = tracklet.locationSequence.length - 256 * increment;
                                        tracklet = tracklet.truncateAndShrink(start, 256, increment);
                                    }
                                    // Recognize attributes.
                                    Attributes attr = attrRecogSingleton.getInst().recognize(tracklet);
                                    logger.debug("Attributes retrieved for task " + taskID + "!");
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
                                        try {
                                            taskData.changeCurNode(topic);
                                        } catch (RecordNotFoundException e) {
                                            logger.warn("When changing node in TaskData", e);
                                        }
                                        sendWithLog(topic,
                                                taskID,
                                                serialize(taskData),
                                                producerSingleton.getInst(),
                                                logger);
                                    }
                                } catch (Exception e) {
                                    loggerSingleton.getInst().error("During processing attributes.", e);
                                }
                            })
                    );
        }
    }
}
