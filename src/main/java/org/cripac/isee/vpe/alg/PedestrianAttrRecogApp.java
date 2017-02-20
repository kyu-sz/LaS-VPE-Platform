/*
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
 */

package org.cripac.isee.vpe.alg;

import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.attr.DeepMAR;
import org.cripac.isee.pedestrian.attr.ExternPedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The PedestrianAttrRecogApp class is a Spark Streaming application which
 * performs pedestrian attribute recognition.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianAttrRecogApp extends SparkStreamingApp {
    private static final long serialVersionUID = 2559258492435661197L;

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }

    enum Algorithm {
        EXT,
        DeepMAR
    }

    /**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-attr-recog";

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianAttrRecogApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new RecogStream(propCenter)));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;
        InetAddress externAttrRecogServerAddr = InetAddress.getLocalHost();
        int externAttrRecogServerPort = 0;
        // Max length of a tracklet to recignize attr from. 0 means not limiting.
        int maxTrackletLength = 0;
        Algorithm algorithm = Algorithm.EXT;

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
                    case "vpe.ped.tracking.max.length":
                        maxTrackletLength = new Integer((String) entry.getValue());
                        break;
                    case "vpe.ped.attr.alg":
                        algorithm = Algorithm.valueOf((String) entry.getValue());
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
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class RecogStream extends Stream {

        public static final String NAME = "recog";
        public static final DataType OUTPUT_TYPE = DataType.ATTRIBUTES;

        /**
         * Topic to input tracklets from Kafka.
         */
        public static final Port TRACKLET_PORT =
                new Port("pedestrian-tracklet-for-attr-recog", DataType.TRACKLET);
        private static final long serialVersionUID = -4672941060404428484L;

        private final Singleton<PedestrianAttrRecognizer> recognizerSingleton;
        // Max length of the resulting tracklet. 0 means not limiting.
        private final int maxTrackletLength;

        public RecogStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            this.maxTrackletLength = propCenter.maxTrackletLength;

            loggerSingleton.getInst().debug("Using Kafka brokers: " + propCenter.kafkaBootstrapServers);

            switch (propCenter.algorithm) {
                case EXT:
                    recognizerSingleton = new Singleton<>(() -> new ExternPedestrianAttrRecognizer(
                            propCenter.externAttrRecogServerAddr,
                            propCenter.externAttrRecogServerPort,
                            loggerSingleton.getInst()));
                    break;
                case DeepMAR:
                    recognizerSingleton = new Singleton<>(() -> new DeepMAR(-1, loggerSingleton.getInst()));
                    break;
                default:
                    throw new NotImplementedException("Recognizer singleton construction for "
                            + propCenter.algorithm + " not realized.");
            }
        }

        @Override
        public void addToGlobalStream(Map<String, JavaPairDStream<String, TaskData>> globalStreamMap) {// Extract tracklets from the data.
            // Recognize attributes from the tracklets.
            this.filter(globalStreamMap, TRACKLET_PORT)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        try {
                            Logger logger = loggerSingleton.getInst();

                            String taskID = kv._1();
                            TaskData taskData = kv._2();
                            logger.debug("Received task " + taskID + "!");

                            Tracklet tracklet = (Tracklet) taskData.predecessorRes;
                            logger.debug("To recognize attributes for task " + taskID + "!");
                            // Truncate and shrink the tracklet in case it is too large.
                            if (maxTrackletLength > 0
                                    && tracklet.locationSequence.length > maxTrackletLength) {
                                final int increment = tracklet.locationSequence.length / maxTrackletLength;
                                final int start =
                                        tracklet.locationSequence.length - maxTrackletLength * increment;
                                tracklet = tracklet.truncateAndShrink(start, maxTrackletLength, increment);
                            }
                            // Recognize attributes.
                            Attributes attr = recognizerSingleton.getInst().recognize(tracklet);
                            logger.debug("Attributes retrieved for task " + taskID + "!");
                            attr.trackletID = tracklet.id;

                            // Find current node.
                            TaskData.ExecutionPlan.Node curNode = taskData.getCurrentNode(TRACKLET_PORT);
                            // Get ports to output to.
                            List<TaskData.ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                            // Mark the current node as executed.
                            curNode.markExecuted();

                            output(outputPorts, taskData.executionPlan, attr, taskID);
                        } catch (Exception e) {
                            loggerSingleton.getInst().error("During processing attributes.", e);
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(TRACKLET_PORT);
        }
    }
}
