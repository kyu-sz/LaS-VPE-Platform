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
import org.apache.kafka.clients.producer.KafkaProducer;
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
import org.cripac.isee.vpe.util.Factory;
import org.cripac.isee.vpe.util.SerializationHelper;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.Tuple2;

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
    public static final String APP_NAME = "MessageHandling";
    private Stream msgHandlingStream;

    /**
     * The constructor method. It sets the configurations, but does not start
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

        TopicManager.checkTopics(propCenter);

        MessageHandlingApp messageHandlingApp = new MessageHandlingApp(propCenter);
        messageHandlingApp.initialize(propCenter);
        messageHandlingApp.start();
        messageHandlingApp.awaitTermination();
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

        private Parameter() {
        }
    }

    /**
     * This class stores possible commands and the String expressions of them.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class CommandType {
        public final static String TRACK_ONLY = "track-only";
        public final static String TRACK_AND_RECOG_ATTR = "track-and-recog-attr";
        public final static String RECOG_ATTR_ONLY = "recog-attr-only";
        public final static String REID_ONLY = "reid-only";
        public final static String RECOG_ATTR_AND_REID = "recog-attr-and-reid";
        public final static String TRACK_AND_RECOG_ATTR_AND_REID = "track-and-recog-attr-and-reid";
    }

    public static class UnsupportedCommandException extends Exception {
        private static final long serialVersionUID = -940732652485656739L;
    }

    public static class MessageHandlingStream extends Stream {

        public static final Info INFO = new Info("MessageHandling", DataType.NONE);
        /**
         * Topic of command.
         */
        public static final Topic COMMAND_TOPIC = new Topic(
                "command", DataType.COMMAND, MessageHandlingStream.INFO);

        private Map<String, Integer> cmdTopicMap = new HashMap<>();
        private Map<String, String> kafkaParams;
        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<SynthesizedLogger> loggerSingleton;
        private Singleton<HDFSReader> hdfsReaderSingleton;

        public MessageHandlingStream(SystemPropertyCenter propCenter) throws Exception {
            cmdTopicMap.put(COMMAND_TOPIC.NAME, propCenter.kafkaNumPartitions);

            kafkaParams = new HashMap<>();
            System.out.println("|INFO|MessageHandlingApp: metadata.broker.list=" + propCenter.kafkaBrokers);
            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            kafkaParams.put("metadata.broker.list", propCenter.kafkaBrokers);
            kafkaParams.put("group.id", "MessageHandlingApp" + UUID.randomUUID());
            // Determine where the stream starts (default: largest)
            kafkaParams.put("auto.offset.reset", "smallest");
            kafkaParams.put("fetch.message.max.bytes", "" + propCenter.kafkaFetchMsgMaxBytes);

            Properties producerProp = new Properties();
            producerProp.put("bootstrap.servers", propCenter.kafkaBrokers);
            producerProp.put("compression.codec", "1");
            producerProp.put("max.request.size", "10000000");
            producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProp.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));
            loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(
                    INFO.NAME,
                    propCenter.verbose ? Level.DEBUG : Level.INFO,
                    propCenter.reportListenerAddr,
                    propCenter.reportListenerPort));
            hdfsReaderSingleton = new Singleton<>(new Factory<HDFSReader>() {
                @Override
                public HDFSReader produce() throws Exception {
                    return new HDFSReader();
                }
            });
        }

        /**
         * Create an execution plan according to given command and parameter.
         *
         * @param cmd   Command specifying the plan.
         * @param param Parameter for creating the plan.
         * @return Execution plan corresponding to the command.
         * @throws UnsupportedCommandException On dealing with unsupported command.
         */
        private ExecutionPlan createPlanByCmdAndParam(String cmd, Map<String, String> param)
                throws UnsupportedCommandException, DataTypeUnmatchException {
            ExecutionPlan plan = new ExecutionPlan();

            switch (cmd) {
                case CommandType.TRACK_ONLY: {
                    // Perform tracking only.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.TrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    // The linkNodes method will automatically add the DataManagingApp node.
                    plan.linkNodes(trackingNode,
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    break;
                }
                case CommandType.RECOG_ATTR_ONLY: {
                    // Retrieve track data, then feed it to attr recog module.
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            DataManagingApp.PedestrainTrackletRetrievingStream.INFO);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    plan.linkNodes(trackletDataNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.linkNodes(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    break;
                }
                case CommandType.TRACK_AND_RECOG_ATTR: {
                    // Do tracking, then output to attr recog module.
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.TrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    plan.linkNodes(trackingNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.linkNodes(trackingNode,
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.linkNodes(attrRecogNode,
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
                    plan.linkNodes(trackWithAttrDataNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_ATTR_TOPIC);
                    plan.linkNodes(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.RECOG_ATTR_AND_REID: {
                    ExecutionPlan.Node trackletDataNode = plan.addNode(
                            DataManagingApp.PedestrainTrackletRetrievingStream.INFO);
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.linkNodes(trackletDataNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.linkNodes(trackletDataNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.linkNodes(attrRecogNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.linkNodes(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.linkNodes(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                case CommandType.TRACK_AND_RECOG_ATTR_AND_REID: {
                    ExecutionPlan.Node trackingNode = plan.addNode(
                            PedestrianTrackingApp.TrackingStream.INFO,
                            param.get(Parameter.TRACKING_CONF_FILE));
                    ExecutionPlan.Node attrRecogNode = plan.addNode(
                            PedestrianAttrRecogApp.RecogStream.INFO);
                    ExecutionPlan.Node reidNode = plan.addNode(
                            PedestrianReIDUsingAttrApp.ReIDStream.INFO);
                    plan.linkNodes(trackingNode,
                            PedestrianAttrRecogApp.RecogStream.TRACKLET_TOPIC);
                    plan.linkNodes(trackingNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_TOPIC);
                    plan.linkNodes(attrRecogNode,
                            PedestrianReIDUsingAttrApp.ReIDStream.ATTR_TOPIC);
                    plan.linkNodes(trackingNode,
                            DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC);
                    plan.linkNodes(attrRecogNode,
                            DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC);
                    plan.linkNodes(reidNode,
                            DataManagingApp.SavingStream.PED_IDRANK_SAVING_TOPIC);
                    break;
                }
                default:
                    throw new UnsupportedCommandException();
            }

            return plan;
        }

        @Override
        public void addToContext(JavaStreamingContext jsc) {// Handle the messages received from Kafka,
            buildBytesDirectStream(jsc, kafkaParams, cmdTopicMap)
                    .foreachRDD(rdd -> {
                        rdd.foreachPartition(msgIter -> {
                            while (msgIter.hasNext()) {
                                UUID taskID = UUID.randomUUID();

                                // Get a next command message.
                                Tuple2<String, byte[]> msg = msgIter.next();
                                String cmd = msg._1();
                                Map<String, String> param = (Map<String, String>) deserialize(msg._2());

                                List<Path> videoPaths =
                                        hdfsReaderSingleton.getInst().listSubfiles(
                                                new Path(param.get(Parameter.VIDEO_URL)));

                                // Create an execution plan according to the command.
                                ExecutionPlan plan = createPlanByCmdAndParam(cmd, param);

                                // For each video to be processed
                                for (Path path : videoPaths) {
                                    // Choose modules to sendWithLog data according to the command.
                                    switch (cmd) {
                                        // These commands need only sending video URLs to the tracking module.
                                        case CommandType.TRACK_ONLY:
                                        case CommandType.TRACK_AND_RECOG_ATTR:
                                        case CommandType.TRACK_AND_RECOG_ATTR_AND_REID: {
                                            TaskData taskData = new TaskData(
                                                    plan.findNode(PedestrianTrackingApp
                                                            .TrackingStream
                                                            .VIDEO_URL_TOPIC),
                                                    plan,
                                                    param.get(Parameter.VIDEO_URL));
                                            sendWithLog(PedestrianTrackingApp.TrackingStream.VIDEO_URL_TOPIC,
                                                    taskID.toString(),
                                                    serialize(taskData),
                                                    producerSingleton.getInst(),
                                                    loggerSingleton.getInst());
                                            break;
                                        }
                                        // These commands need only sending tracklet IDs to the
                                        // data managing module to retrieve tracklets.
                                        case CommandType.RECOG_ATTR_ONLY:
                                        case CommandType.RECOG_ATTR_AND_REID: {
                                            Tracklet.Identifier id = new Tracklet.Identifier(
                                                    path.toString(),
                                                    Integer.valueOf(param.get(
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
                                                    Integer.valueOf(param.get(
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
                            }
                        });
                    });
        }
    }
}
