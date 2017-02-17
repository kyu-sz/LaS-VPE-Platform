/*
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
 */

package org.cripac.isee.vpe.ctrl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.cripac.isee.vpe.alg.PedestrianReIDUsingAttrApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp.HDFSVideoTrackingStream;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.common.Topic;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.DataManagingApp;
import org.cripac.isee.vpe.data.DataManagingApp.PedestrainTrackletAttrRetrievingStream;
import org.cripac.isee.vpe.data.DataManagingApp.PedestrainTrackletRetrievingStream;
import org.cripac.isee.vpe.data.HDFSReader;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The MessageHandlingApp class is a Spark Streaming application responsible for
 * receiving commands from sources like web-UI, then producing appropriate
 * command messages and sending to command-defined starting application.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class MessageHandlingApp extends SparkStreamingApp {
    /**
     * The NAME of this application.
     */
    public static final String APP_NAME = "message-handling";

    /**
     * The constructor method. It sets the configurations, but does not run
     * the contexts.
     *
     * @param propCenter The propCenter stores all the available configurations.
     * @throws Exception Any exception that might occur during execution.
     */
    public MessageHandlingApp(SystemPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new MessageHandlingStream(propCenter)));
    }

    public static void main(String[] args) throws Exception {
        SystemPropertyCenter propCenter;
        if (args.length > 0) {
            propCenter = new SystemPropertyCenter(args);
        } else {
            propCenter = new SystemPropertyCenter();
        }

        SparkStreamingApp app = new MessageHandlingApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    /**
     * The class Parameter contains a numeration of parameter types
     * the MessageHandlingApp may use, as well as their keys.
     */
    public static class Parameter {
        public final static String VIDEO_URL = "video-url";
        public final static String TRACKING_CONF_FILE = "tracking-conf-file";
        public final static String TRACKLET_INDEX = "tracklet-serial-num";
        public final static String WEBCAM_LOGIN_PARAM = "webcam-login-param";

        private Parameter() {
        }
    }

    /**
     * This class stores possible commands and the String expressions of them.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class CommandType {
        public final static String TRACK_ONLY = "track";
        public final static String TRACK_ATTRRECOG = "track-attrrecog";
        public final static String ATTRRECOG_ONLY = "attrrecog";
        public final static String REID_ONLY = "reid";
        public final static String ATTRRECOG_REID = "attrrecog-reid";
        public final static String TRACK_ATTRRECOG_REID = "track-attrrecog-reid";
        public final static String RT_TRACK_ONLY = "rttrack";
        public final static String RT_TRACK_ATTRRECOG_REID = "rt-track-attrrecog-reid";
    }

    public static class UnsupportedCommandException extends Exception {
        private static final long serialVersionUID = -940732652485656739L;
    }

    public static class MessageHandlingStream extends Stream {

        public static final String NAME = "msg-handling";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        /**
         * Topic of command.
         */
        public static final Topic COMMAND_TOPIC = new Topic(
                "command", DataType.COMMAND);
        private static final long serialVersionUID = -8438559854398738231L;

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<HDFSReader> hdfsReaderSingleton;

        public MessageHandlingStream(SystemPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            Properties producerProp = propCenter.getKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));

            hdfsReaderSingleton = new Singleton<>(HDFSReader::new);
        }

        private void handle(String cmd, Map<String, Serializable> param, String taskID) throws Exception {
            final KafkaProducer<String, byte[]> producer = producerSingleton.getInst();
            final Logger logger = loggerSingleton.getInst();
            final ExecutionPlan plan = new ExecutionPlan();
            // Process stored videos.
            final List<Path> videoPaths = hdfsReaderSingleton.getInst().listSubfiles(
                    new Path((String) param.get(Parameter.VIDEO_URL)));

            switch (cmd) {
                case CommandType.TRACK_ONLY: {
                    // Perform tracking only.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            HDFSVideoTrackingStream.OUTPUT_TYPE,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node trackletSavingNode = plan.addNode(
                            DataManagingApp.TrackletSavingStream.OUTPUT_TYPE);
                    // The letNodeOutputTo method will automatically add the DataManagingApp node.
                    plan.letNodeOutputTo(trackingNode,
                            trackletSavingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    videoPaths.forEach(path -> {
                        final TaskData<String> taskData = new TaskData<>(trackingNode, plan, path.toString());
                        try {
                            sendWithLog(HDFSVideoTrackingStream.VIDEO_URL_TOPIC,
                                    taskID, serialize(taskData), producer, logger);
                        } catch (IOException e) {
                            logger.error("On serializing TaskData", e);
                        }
                    });
                    break;
                }
                case CommandType.TRACK_ATTRRECOG: {
                    // Do tracking, then output to attr recog module.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.HDFSVideoTrackingStream.OUTPUT_TYPE,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                    ExecutionPlan.Node trackletSavingNode = plan.addNode(
                            DataManagingApp.TrackletSavingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node attrSavingNode = plan.addNode(
                            DataManagingApp.AttrSavingStream.OUTPUT_TYPE);
                    plan.letNodeOutputTo(trackingNode,
                            attrRecogNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            trackletSavingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            attrSavingNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    videoPaths.forEach(path -> {
                        final TaskData<String> taskData = new TaskData<>(trackingNode, plan, path.toString());
                        try {
                            sendWithLog(HDFSVideoTrackingStream.VIDEO_URL_TOPIC,
                                    taskID, serialize(taskData), producer, logger);
                        } catch (IOException e) {
                            logger.error("On serializing TaskData", e);
                        }
                    });
                    break;
                }
                case CommandType.TRACK_ATTRRECOG_REID: {
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            HDFSVideoTrackingStream.OUTPUT_TYPE,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.OUTPUT_TYPE);
                    ExecutionPlan.Node trackletSavingNode = plan.addNode(
                            DataManagingApp.TrackletSavingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node attrSavingNode = plan.addNode(
                            DataManagingApp.AttrSavingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node idRankSavingNode = plan.addNode(
                            DataManagingApp.IDRankSavingStream.OUTPUT_TYPE);
                    plan.letNodeOutputTo(trackingNode,
                            attrRecogNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            reidNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            reidNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            trackletSavingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            attrSavingNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            idRankSavingNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    videoPaths.forEach(path -> {
                        final TaskData<String> taskData = new TaskData<>(trackingNode, plan, path.toString());
                        try {
                            sendWithLog(HDFSVideoTrackingStream.VIDEO_URL_TOPIC,
                                    taskID, serialize(taskData), producer, logger);
                        } catch (IOException e) {
                            logger.error("On serializing TaskData", e);
                        }
                    });
                    break;
                }
                case CommandType.ATTRRECOG_ONLY: {
                    // Retrieve track data, then feed it to attr recog module.
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            PedestrainTrackletRetrievingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                    ExecutionPlan.Node attrSavingNode = plan.addNode(
                            DataManagingApp.AttrSavingStream.OUTPUT_TYPE);
                    plan.letNodeOutputTo(trackletDataNode,
                            trackletDataNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            attrSavingNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    String trackletIdx = (String) param.get(Parameter.TRACKLET_INDEX);
                    videoPaths.forEach(path -> {
                        Tracklet.Identifier id = new Tracklet.Identifier(
                                path.toString(),
                                Integer.valueOf(trackletIdx));
                        final TaskData<Tracklet.Identifier> taskData = new TaskData<>(trackletDataNode, plan, id);
                        try {
                            sendWithLog(PedestrainTrackletRetrievingStream.RTRV_JOB_TOPIC,
                                    taskID, serialize(taskData), producer, logger);
                        } catch (IOException e) {
                            logger.error("On serializing TaskData", e);
                        }
                    });
                    break;
                }
                case CommandType.ATTRRECOG_REID: {
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            PedestrainTrackletRetrievingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.OUTPUT_TYPE);
                    ExecutionPlan.Node attrSavingNode = plan.addNode(
                            DataManagingApp.AttrSavingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node idRankSavingNode = plan.addNode(
                            DataManagingApp.IDRankSavingStream.OUTPUT_TYPE);
                    plan.letNodeOutputTo(trackletDataNode,
                            attrRecogNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackletDataNode,
                            reidNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            reidNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            attrSavingNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            idRankSavingNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    String trackletIdx = (String) param.get(Parameter.TRACKLET_INDEX);
                    videoPaths.forEach(path -> {
                        Tracklet.Identifier id = new Tracklet.Identifier(
                                path.toString(),
                                Integer.valueOf(trackletIdx));
                        final TaskData<Tracklet.Identifier> taskData = new TaskData<>(trackletDataNode, plan, id);
                        try {
                            sendWithLog(PedestrainTrackletRetrievingStream.RTRV_JOB_TOPIC,
                                    taskID, serialize(taskData), producer, logger);
                        } catch (IOException e) {
                            logger.error("On serializing TaskData", e);
                        }
                    });
                    break;
                }
                case CommandType.REID_ONLY: {
                    // Retrieve track and attr data integrally, then feed them to ReID
                    // module.
                    ExecutionPlan.Node trackWithAttrDataNode = plan.addNode(
                            PedestrainTrackletAttrRetrievingStream.OUTPUT_TYPE);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.OUTPUT_TYPE);
                    ExecutionPlan.Node idRankSavingNode = plan.addNode(
                            DataManagingApp.IDRankSavingStream.OUTPUT_TYPE);
                    plan.letNodeOutputTo(trackWithAttrDataNode,
                            reidNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_ATTR_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            idRankSavingNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    String trackletIdx = (String) param.get(Parameter.TRACKLET_INDEX);
                    videoPaths.forEach(path -> {
                        Tracklet.Identifier id = new Tracklet.Identifier(
                                path.toString(),
                                Integer.valueOf(trackletIdx));
                        final TaskData<Tracklet.Identifier> taskData = new TaskData<>(trackWithAttrDataNode, plan, id);
                        try {
                            sendWithLog(PedestrainTrackletAttrRetrievingStream.RTRV_JOB_TOPIC,
                                    taskID, serialize(taskData), producer, logger);
                        } catch (IOException e) {
                            logger.error("On serializing TaskData", e);
                        }
                    });
                    break;
                }
                default:
                    throw new UnsupportedCommandException();
            }
        }

        // Handle the messages received from Kafka,
        @Override
        public void addToStream(JavaDStream<ConsumerRecord<String, byte[]>> globalStream) {
            this.<Hashtable<String, Serializable>>filter(globalStream, COMMAND_TOPIC)
                    .foreachRDD(rdd -> rdd.foreach(msg -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            String taskID = UUID.randomUUID().toString();

                            // Get a next command message.
                            String cmd = msg._1();
                            logger.debug("Received command: " + cmd);

                            final Hashtable<String, Serializable> param = msg._2();

                            if (cmd.equals(CommandType.RT_TRACK_ONLY)
                                    || cmd.equals(CommandType.RT_TRACK_ATTRRECOG_REID)) {
                                // TODO: After finishing real time processing function, implement here.
                                throw new NotImplementedException();
                            } else {
                                handle(cmd, param, taskID);
                            }
                        } catch (Exception e) {
                            logger.error("During msg handling", e);
                        }
                    }));
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(COMMAND_TOPIC.NAME);
        }
    }
}
