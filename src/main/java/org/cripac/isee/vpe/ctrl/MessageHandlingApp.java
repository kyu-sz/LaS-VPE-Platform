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

import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.cripac.isee.vpe.alg.PedestrianReIDUsingAttrApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.DataManagingApp;
import org.cripac.isee.vpe.data.HDFSReader;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import java.io.Serializable;
import java.util.*;

import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;
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
        app.initialize(propCenter);
        app.start();
        app.awaitTermination();
    }

    @Override
    protected JavaStreamingContext getStreamContext() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf(true));
        sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
        JavaStreamingContext jsc =
                new JavaStreamingContext(sparkContext, Durations.seconds(1));

        msgHandlingStream.addToContext(jsc);

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

        public static final Info INFO = new Info("msg-handling", DataType.NONE);
        /**
         * Topic of command.
         */
        public static final Topic COMMAND_TOPIC = new Topic(
                "command", DataType.COMMAND, MessageHandlingStream.INFO);

        private Map<String, String> kafkaParams;
        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<HDFSReader> hdfsReaderSingleton;
        private final int procTime;

        public MessageHandlingStream(SystemPropertyCenter propCenter) throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(INFO.NAME,
                    propCenter.verbose ? Level.DEBUG : Level.INFO,
                    propCenter.reportListenerAddr,
                    propCenter.reportListenerPort)));

            this.procTime = propCenter.procTime;

            kafkaParams = new HashMap<>();
            loggerSingleton.getInst().debug("bootstrap.servers="
                    + propCenter.kafkaBootstrapServers);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    propCenter.kafkaBootstrapServers);
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,
                    INFO.NAME);
            kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "largest");
            kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                    "" + propCenter.kafkaFetchMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());

            Properties producerProp = new Properties();
            producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    propCenter.kafkaBootstrapServers);
            producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    propCenter.kafkaMaxRequestSize);
            producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());

            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));
            hdfsReaderSingleton = new Singleton<>(() -> new HDFSReader());
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
                throws UnsupportedCommandException, DataTypeUnmatchException {
            ExecutionPlan plan = new ExecutionPlan();

            switch (cmd) {
                case CommandType.TRACK_ONLY: {
                    // Perform tracking only.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.VideoFragmentTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    // The letNodeOutputTo method will automatically add the DataManagingApp node.
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    break;
                }
                case CommandType.RT_TRACK_ONLY: {
                    // Perform tracking only.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.RTVideoStreamTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    // The letNodeOutputTo method will automatically add the DataManagingApp node.
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    break;
                }
                case CommandType.ATTRRECOG_ONLY: {
                    // Retrieve track data, then feed it to attr recog module.
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            DataManagingApp.PedestrainTrackletRetrievingStream.INFO);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    plan.letNodeOutputTo(trackletDataNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    break;
                }
                case CommandType.TRACK_ATTRRECOG: {
                    // Do tracking, then output to attr recog module.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.VideoFragmentTrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    plan.letNodeOutputTo(trackingNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.letNodeOutputTo(trackingNode,
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    break;
                }
                case CommandType.REID_ONLY: {
                    // Retrieve track and attr data integrally, then feed them to ReID
                    // module.
                    ExecutionPlan.Node trackWithAttrDataNode = plan.addNode(
                            DataManagingApp.PedestrainTrackletAttrRetrievingStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.letNodeOutputTo(trackWithAttrDataNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_ATTR_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.ATTRRECOG_REID: {
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            DataManagingApp.PedestrainTrackletRetrievingStream.INFO);
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
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.TRACK_ATTRRECOG_REID: {
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.VideoFragmentTrackingStream.INFO,
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
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.RT_TRACK_ATTRRECOG_REID: {
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.RTVideoStreamTrackingStream.INFO,
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
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.letNodeOutputTo(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.letNodeOutputTo(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                default:
                    throw new UnsupportedCommandException();
            }

            return plan;
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {// Handle the messages received from Kafka,
            buildBytesDirectStream(jssc, Arrays.asList(COMMAND_TOPIC.NAME), kafkaParams, procTime)
                    .foreachRDD(rdd ->
                            rdd.foreach(msg -> {
                                UUID taskID = UUID.randomUUID();

                                // Get a next command message.
                                String cmd = msg._1();
                                Hashtable<String, Serializable> param;
                                {
                                    Object tmp = deserialize(msg._2());
                                    if (!(tmp instanceof Hashtable)) {
                                        loggerSingleton.getInst().error("Expecting Map but received " + tmp);
                                        return;
                                    }
                                    param = (Hashtable<String, Serializable>) tmp;
                                }

                                loggerSingleton.getInst().debug("Received command: " + cmd);

                                switch (cmd) {
                                    case CommandType.RT_TRACK_ONLY:
                                    case CommandType.RT_TRACK_ATTRRECOG_REID: {
                                        // Process real-time data.
                                        ExecutionPlan plan = createPlanByCmdAndParam(cmd, param);
                                        TaskData taskData = new TaskData(
                                                plan.findNode(PedestrianTrackingApp.RTVideoStreamTrackingStream.LOGIN_PARAM_TOPIC),
                                                plan,
                                                param.get(Parameter.WEBCAM_LOGIN_PARAM));
                                        sendWithLog(
                                                PedestrianTrackingApp.RTVideoStreamTrackingStream.LOGIN_PARAM_TOPIC,
                                                taskID.toString(),
                                                serialize(taskData),
                                                producerSingleton.getInst(),
                                                loggerSingleton.getInst());
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
                                                hdfsReaderSingleton.getInst().listSubfiles(
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
                                                    TaskData taskData = new TaskData(
                                                            plan.findNode(
                                                                    PedestrianTrackingApp.VideoFragmentTrackingStream
                                                                            .VIDEO_URL_TOPIC),
                                                            plan,
                                                            param.get(Parameter.VIDEO_URL));
                                                    sendWithLog(
                                                            PedestrianTrackingApp.VideoFragmentTrackingStream
                                                                    .VIDEO_URL_TOPIC,
                                                            taskID.toString(),
                                                            serialize(taskData),
                                                            producerSingleton.getInst(),
                                                            loggerSingleton.getInst());
                                                    break;
                                                }
                                                // These commands need only sending tracklet IDs to the
                                                // data managing module to retrieve tracklets.
                                                case CommandType.ATTRRECOG_ONLY:
                                                case CommandType.ATTRRECOG_REID: {
                                                    Tracklet.Identifier id = new Tracklet.Identifier(
                                                            path.toString(),
                                                            Integer.valueOf((String) param.get(
                                                                    Parameter.TRACKLET_SERIAL_NUM)));
                                                    TaskData taskData = new TaskData(
                                                            plan.findNode(DataManagingApp
                                                                    .PedestrainTrackletRetrievingStream
                                                                    .PED_TRACKLET_RTRV_JOB_TOPIC),
                                                            plan,
                                                            id);
                                                    sendWithLog(DataManagingApp
                                                                    .PedestrainTrackletRetrievingStream
                                                                    .PED_TRACKLET_RTRV_JOB_TOPIC,
                                                            taskID.toString(),
                                                            serialize(taskData),
                                                            producerSingleton.getInst(),
                                                            loggerSingleton.getInst());
                                                    break;
                                                }
                                                // This command needs only sending tracklet IDs to the
                                                // data managing module to retrieve tracklets and attributes.
                                                case CommandType.REID_ONLY: {
                                                    Tracklet.Identifier id = new Tracklet.Identifier(
                                                            path.toString(),
                                                            Integer.valueOf((String) param.get(
                                                                    Parameter.TRACKLET_SERIAL_NUM)));
                                                    TaskData taskData = new TaskData(
                                                            plan.findNode(DataManagingApp
                                                                    .PedestrainTrackletAttrRetrievingStream
                                                                    .JOB_TOPIC),
                                                            plan,
                                                            id);
                                                    sendWithLog(DataManagingApp
                                                                    .PedestrainTrackletAttrRetrievingStream
                                                                    .JOB_TOPIC,
                                                            taskID.toString(),
                                                            serialize(taskData),
                                                            producerSingleton.getInst(),
                                                            loggerSingleton.getInst());
                                                    break;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                }
                            })
                    );
        }
    }
}
