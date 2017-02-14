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

package org.cripac.isee.vpe.ctrl;

import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.cripac.isee.vpe.alg.PedestrianReIDUsingAttrApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp.HDFSVideoTrackingStream;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp.RTVideoStreamTrackingStream;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.DataManagingApp;
import org.cripac.isee.vpe.data.DataManagingApp.PedestrainTrackletAttrRetrievingStream;
import org.cripac.isee.vpe.data.DataManagingApp.PedestrainTrackletRetrievingStream;
import org.cripac.isee.vpe.data.HDFSReader;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import java.io.Serializable;
import java.util.*;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
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
    private Stream msgHandlingStream;

    /**
     * The constructor method. It sets the configurations, but does not run
     * the contexts.
     *
     * @param propCenter The propCenter stores all the available configurations.
     * @throws Exception Any exception that might occur during execution.
     */
    public MessageHandlingApp(SystemPropertyCenter propCenter) throws Exception {
        super(propCenter);
        msgHandlingStream = new MessageHandlingStream(propCenter);
    }

    public static void main(String[] args) throws Exception {
        SystemPropertyCenter propCenter;
        if (args.length > 0) {
            propCenter = new SystemPropertyCenter(args);
        } else {
            propCenter = new SystemPropertyCenter();
        }

        SparkStreamingApp app = new MessageHandlingApp(propCenter);
        TopicManager.checkTopics(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    @Override
    protected JavaStreamingContext getStreamContext() {
        JavaStreamingContext jssc = super.getStreamContext();
        msgHandlingStream.addToContext(jssc);
        return jssc;
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

    /**
     * The class Parameter contains a numeration of parameter types
     * the MessageHandlingApp may use, as well as their keys.
     */
    public static class Parameter {
        public final static String VIDEO_URL = "video-url";
        public final static String TRACKING_CONF_FILE = "tracking-conf-file";
        public final static String TRACKLET_SERIAL_NUM = "tracklet-serial-num";
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

        public static final Info INFO = new Info("msg-handling", DataTypes.NONE);
        /**
         * Topic of command.
         */
        public static final Topic COMMAND_TOPIC = new Topic(
                "command", DataTypes.COMMAND, MessageHandlingStream.INFO);
        private static final long serialVersionUID = -8438559854398738231L;

        private Map<String, String> kafkaParams;
        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<HDFSReader> hdfsReaderSingleton;

        public MessageHandlingStream(SystemPropertyCenter propCenter) throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            kafkaParams = propCenter.getKafkaParams(INFO.NAME);

            Properties producerProp = propCenter.getKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));

            hdfsReaderSingleton = new Singleton<>(HDFSReader::new);
        }

        /**
         * Create an execution plan according to given command and parameter.
         *
         * @param cmd   Command specifying the plan.
         * @param param Parameter for creating the plan.
         * @return Execution plan corresponding to the command.
         * @throws UnsupportedCommandException On dealing with unsupported command.
         */
        private ExecutionPlan createPlanByCmdAndParam(String cmd, Map<String, Serializable> param)
                throws UnsupportedCommandException, DataTypeNotMatchedException {
            ExecutionPlan plan = new ExecutionPlan();

            switch (cmd) {
                case CommandType.TRACK_ONLY: {
                    // Perform tracking only.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.HDFSVideoTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    // The letNodeOutputTo method will automatically add the DataManagingApp node.
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    break;
                }
                case CommandType.RT_TRACK_ONLY: {
                    // Perform tracking only.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            RTVideoStreamTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    // The letNodeOutputTo method will automatically add the DataManagingApp node.
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    break;
                }
                case CommandType.ATTRRECOG_ONLY: {
                    // Retrieve track data, then feed it to attr recog module.
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            PedestrainTrackletRetrievingStream.INFO);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    plan.letNodeOutputTo(trackletDataNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    break;
                }
                case CommandType.TRACK_ATTRRECOG: {
                    // Do tracking, then output to attr recog module.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.HDFSVideoTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    plan.letNodeOutputTo(trackingNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    break;
                }
                case CommandType.REID_ONLY: {
                    // Retrieve track and attr data integrally, then feed them to ReID
                    // module.
                    ExecutionPlan.Node trackWithAttrDataNode = plan.addNode(
                            PedestrainTrackletAttrRetrievingStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.letNodeOutputTo(trackWithAttrDataNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_ATTR_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.ATTRRECOG_REID: {
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            PedestrainTrackletRetrievingStream.INFO);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.letNodeOutputTo(trackletDataNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackletDataNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.TRACK_ATTRRECOG_REID: {
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            HDFSVideoTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.letNodeOutputTo(trackingNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.RT_TRACK_ATTRRECOG_REID: {
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            RTVideoStreamTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.letNodeOutputTo(trackingNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                default:
                    throw new UnsupportedCommandException();
            }

            return plan;
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {// Handle the messages received from Kafka,
            final KafkaCluster kc = KafkaHelper.createKafkaCluster(kafkaParams);
            buildBytesDirectStream(jssc, Collections.singletonList(COMMAND_TOPIC.NAME), kc)
                    .foreachRDD(rdd -> {
                        rdd.foreach(msg -> {
                            try {
                                final KafkaProducer<String, byte[]> producer = producerSingleton.getInst();
                                final Logger logger = loggerSingleton.getInst();

                                String taskID = UUID.randomUUID().toString();

                                // Get a next command message.
                                String cmd = msg._1();
                                logger.debug("Received command: " + cmd);

                                final Hashtable<String, Serializable> param = deserialize(msg._2());

                                switch (cmd) {
                                    case CommandType.RT_TRACK_ONLY:
                                    case CommandType.RT_TRACK_ATTRRECOG_REID: {
                                        // Process real-time data.
                                        ExecutionPlan plan = createPlanByCmdAndParam(cmd, param);
                                        Serializable tmp = param.get(Parameter.WEBCAM_LOGIN_PARAM);
                                        if (!(tmp instanceof String)) {
                                            throw new DataTypeNotMatchedException(
                                                    "Expecting a String but received " + tmp);
                                        }
                                        TaskData taskData = new TaskData(
                                                plan.findNode(RTVideoStreamTrackingStream.LOGIN_PARAM_TOPIC),
                                                plan, new Gson().fromJson((String) tmp, LoginParam.class));
                                        sendWithLog(RTVideoStreamTrackingStream.LOGIN_PARAM_TOPIC,
                                                taskID, serialize(taskData), producer, logger);
                                        break;
                                    }
                                    case CommandType.TRACK_ONLY:
                                    case CommandType.TRACK_ATTRRECOG:
                                    case CommandType.TRACK_ATTRRECOG_REID:
                                    case CommandType.ATTRRECOG_ONLY:
                                    case CommandType.ATTRRECOG_REID:
                                    case CommandType.REID_ONLY: {
                                        // Process stored videos.
                                        List<Path> videoPaths =
                                                hdfsReaderSingleton.getInst()
                                                        .listSubfiles(
                                                                new Path((String) param.get(Parameter.VIDEO_URL)));

                                        // Create an execution plan according to the command.
                                        ExecutionPlan plan = createPlanByCmdAndParam(cmd, param);

                                        // For each video to be processed
                                        for (Path path : videoPaths) {
                                            // Choose modules to send data to according to the command.
                                            switch (cmd) {
                                                // These commands need to send only video URLs to
                                                // the tracking module.
                                                case CommandType.TRACK_ONLY:
                                                case CommandType.TRACK_ATTRRECOG:
                                                case CommandType.TRACK_ATTRRECOG_REID: {
                                                    Serializable tmp = param.get(Parameter.VIDEO_URL);
                                                    if (!(tmp instanceof String)) {
                                                        throw new DataTypeNotMatchedException(
                                                                "Expecting a String but received " + tmp);
                                                    }
                                                    TaskData taskData = new TaskData(
                                                            plan.findNode(
                                                                    HDFSVideoTrackingStream.VIDEO_URL_TOPIC),
                                                            plan,
                                                            tmp);
                                                    sendWithLog(HDFSVideoTrackingStream.VIDEO_URL_TOPIC,
                                                            taskID, serialize(taskData), producer, logger);
                                                    break;
                                                }
                                                // These commands need only sending tracklet IDs to the
                                                // data managing module to retrieve tracklets.
                                                case CommandType.ATTRRECOG_ONLY:
                                                case CommandType.ATTRRECOG_REID: {
                                                    Serializable tmp = param.get(Parameter.TRACKLET_SERIAL_NUM);
                                                    if (!(tmp instanceof String)) {
                                                        throw new DataTypeNotMatchedException(
                                                                "Expecting a String but received " + tmp);
                                                    }
                                                    Tracklet.Identifier id = new Tracklet.Identifier(
                                                            path.toString(),
                                                            Integer.valueOf((String) tmp));
                                                    TaskData taskData = new TaskData(
                                                            plan.findNode(PedestrainTrackletRetrievingStream
                                                                    .RTRV_JOB_TOPIC),
                                                            plan,
                                                            id);
                                                    sendWithLog(PedestrainTrackletRetrievingStream.RTRV_JOB_TOPIC,
                                                            taskID, serialize(taskData), producer, logger);
                                                    break;
                                                }
                                                // This command needs only sending tracklet IDs to the
                                                // data managing module to retrieve tracklets and attributes.
                                                case CommandType.REID_ONLY: {
                                                    Serializable tmp = param.get(Parameter.TRACKLET_SERIAL_NUM);
                                                    if (!(tmp instanceof String)) {
                                                        throw new DataTypeNotMatchedException(
                                                                "Expecting a String but received " + tmp);
                                                    }
                                                    Tracklet.Identifier id = new Tracklet.Identifier(
                                                            path.toString(),
                                                            Integer.valueOf((String) tmp));
                                                    TaskData taskData = new TaskData(
                                                            plan.findNode(PedestrainTrackletAttrRetrievingStream
                                                                    .RTRV_JOB_TOPIC),
                                                            plan,
                                                            id);
                                                    sendWithLog(PedestrainTrackletAttrRetrievingStream.RTRV_JOB_TOPIC,
                                                            taskID, serialize(taskData), producer, logger);
                                                    break;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                }
                            } catch (Exception e) {
                                loggerSingleton.getInst().error("During msg handling", e);
                            }
                        });

                        KafkaHelper.submitOffset(kc, offsetRanges.get());
                    });
        }
    }
}
