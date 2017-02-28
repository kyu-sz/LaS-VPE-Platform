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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.Function0;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.cripac.isee.vpe.alg.PedestrianReIDUsingAttrApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp.HDFSVideoTrackingStream;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.DataManagingApp;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;
import org.cripac.isee.vpe.data.HDFSReader;
import org.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.tracking.TrackletOrURL;

import java.io.Serializable;
import java.util.*;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
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
     * The name of this application.
     */
    public static final String APP_NAME = "message-handling";
    private static final long serialVersionUID = 4894389080346176479L;

    private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
    private Singleton<HDFSReader> hdfsReaderSingleton;

    /**
     * The constructor method. It sets the configurations, but does not run
     * the contexts.
     *
     * @param propCenter The propCenter stores all the available configurations.
     * @throws Exception Any exception that might occur during execution.
     */
    public MessageHandlingApp(SystemPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        Properties producerProp = propCenter.getKafkaProducerProp(false);
        producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));

        hdfsReaderSingleton = new Singleton<>(HDFSReader::new);
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

        private CommandType() {
        }
    }

    public static class UnsupportedCommandException extends Exception {
        private static final long serialVersionUID = -940732652485656739L;
    }

    @Override
    public void addToContext() throws SparkException {
        checkTopics(Collections.singleton(DataType.COMMAND));

        buildDirectStream(Collections.singleton(DataType.COMMAND))
                .foreachRDD(rdd -> rdd.foreach(rec -> {
                    final Logger logger = loggerSingleton.getInst();
                    try {
                        String taskID = UUID.randomUUID().toString();

                        // Get a next command message.
                        final DataType dataType = rec._1();
                        assert dataType.equals(DataType.COMMAND);
                        final String cmd = rec._2()._1();
                        logger.debug("Received command: " + cmd);

                        final Object2ObjectOpenHashMap<String, Serializable> param = deserialize(rec._2()._2());

                        if (cmd.equals(CommandType.RT_TRACK_ONLY)
                                || cmd.equals(CommandType.RT_TRACK_ATTRRECOG_REID)) {
                            // TODO: After finishing real time processing function, implement here.
                            throw new NotImplementedException();
                        } else {
                            new RobustExecutor<Void, Void>(() -> handle(cmd, param, taskID)).execute();
                        }
                    } catch (Exception e) {
                        logger.error("During msg handling", e);
                    }
                }));
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
                ExecutionPlan.Node trackletSavingNode = plan.addNode(DataManagingApp.TrackletSavingStream.OUTPUT_TYPE);

                // The letNodeOutputTo method will automatically add the DataManagingApp node.
                trackingNode.outputTo(trackletSavingNode.createInputPort(
                        DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_PORT));

                videoPaths.forEach(path -> {
                    final TaskData taskData = new TaskData(
                            trackingNode.createInputPort(HDFSVideoTrackingStream.VIDEO_URL_PORT),
                            plan,
                            path.toString());
                    sendWithLog(taskID, taskData, producer, logger);
                });
                break;
            }
            case CommandType.TRACK_ATTRRECOG: {
                // Do tracking, then output to attr recog module.
                ExecutionPlan.Node trackingNode = plan.addNode(
                        PedestrianTrackingApp.HDFSVideoTrackingStream.OUTPUT_TYPE,
                        param.get(Parameter.TRACKING_CONF_FILE));
                ExecutionPlan.Node attrRecogNode = plan.addNode(PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                ExecutionPlan.Node trackletSavingNode = plan.addNode(DataManagingApp.TrackletSavingStream.OUTPUT_TYPE);
                ExecutionPlan.Node attrSavingNode = plan.addNode(DataManagingApp.AttrSavingStream.OUTPUT_TYPE);

                trackingNode.outputTo(attrRecogNode.createInputPort(
                        PedestrianAttrRecogApp.RecogStream.TRACKLET_PORT));
                trackingNode.outputTo(trackletSavingNode.createInputPort(
                        DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_PORT));
                attrRecogNode.outputTo(attrSavingNode.createInputPort(
                        DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_PORT));

                videoPaths.forEach(path -> {
                    final TaskData taskData = new TaskData(
                            trackingNode.createInputPort(HDFSVideoTrackingStream.VIDEO_URL_PORT),
                            plan,
                            path.toString());
                    sendWithLog(taskID, taskData, producer, logger);
                });
                break;
            }
            case CommandType.TRACK_ATTRRECOG_REID: {
                ExecutionPlan.Node trackingNode = plan.addNode(
                        HDFSVideoTrackingStream.OUTPUT_TYPE,
                        param.get(Parameter.TRACKING_CONF_FILE));
                ExecutionPlan.Node attrRecogNode = plan.addNode(PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                ExecutionPlan.Node reidNode = plan.addNode(PedestrianReIDUsingAttrApp.ReIDStream.OUTPUT_TYPE);
                ExecutionPlan.Node trackletSavingNode = plan.addNode(DataManagingApp.TrackletSavingStream.OUTPUT_TYPE);
                ExecutionPlan.Node attrSavingNode = plan.addNode(DataManagingApp.AttrSavingStream.OUTPUT_TYPE);
                ExecutionPlan.Node idRankSavingNode = plan.addNode(DataManagingApp.IDRankSavingStream.OUTPUT_TYPE);

                trackingNode.outputTo(attrRecogNode.createInputPort(
                        PedestrianAttrRecogApp.RecogStream.TRACKLET_PORT));
                trackingNode.outputTo(reidNode.createInputPort(
                        PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_PORT));
                attrRecogNode.outputTo(reidNode.createInputPort(
                        PedestrianReIDUsingAttrApp.ReIDStream.ATTR_PORT));
                trackingNode.outputTo(trackletSavingNode.createInputPort(
                        DataManagingApp.TrackletSavingStream.PED_TRACKLET_SAVING_PORT));
                attrRecogNode.outputTo(attrSavingNode.createInputPort(
                        DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_PORT));
                reidNode.outputTo(idRankSavingNode.createInputPort(
                        DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_PORT));

                videoPaths.forEach(path -> {
                    final TaskData taskData = new TaskData(
                            trackingNode.createInputPort(HDFSVideoTrackingStream.VIDEO_URL_PORT),
                            plan,
                            path.toString());
                    sendWithLog(taskID, taskData, producer, logger);
                });
                break;
            }
            case CommandType.ATTRRECOG_ONLY: {
                ExecutionPlan.Node attrRecogNode = plan.addNode(PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                ExecutionPlan.Node attrSavingNode = plan.addNode(DataManagingApp.AttrSavingStream.OUTPUT_TYPE);

                attrRecogNode.outputTo(attrSavingNode.createInputPort(
                        DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_PORT));

                String trackletIdx = (String) param.get(Parameter.TRACKLET_INDEX);
                final GraphDatabaseConnector dbConnector = new FakeDatabaseConnector();
                videoPaths.forEach(path -> {
                    final Tracklet.Identifier id = new Tracklet.Identifier(
                            path.toString(),
                            Integer.valueOf(trackletIdx));
                    final TrackletOrURL url = new TrackletOrURL(dbConnector.getTrackletSavingDir(id.videoID)
                            + "/" + id.serialNumber);
                    final TaskData taskData = new TaskData(
                            attrRecogNode.createInputPort(PedestrianAttrRecogApp.RecogStream.TRACKLET_PORT),
                            plan,
                            url);
                    sendWithLog(taskID, taskData, producer, logger);
                });
                break;
            }
            case CommandType.ATTRRECOG_REID: {
                ExecutionPlan.Node attrRecogNode = plan.addNode(PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
                ExecutionPlan.Node reidNode = plan.addNode(PedestrianReIDUsingAttrApp.ReIDStream.OUTPUT_TYPE);
                ExecutionPlan.Node attrSavingNode = plan.addNode(DataManagingApp.AttrSavingStream.OUTPUT_TYPE);
                ExecutionPlan.Node idRankSavingNode = plan.addNode(DataManagingApp.IDRankSavingStream.OUTPUT_TYPE);

                attrRecogNode.outputTo(reidNode.createInputPort(
                        PedestrianReIDUsingAttrApp.ReIDStream.ATTR_PORT));
                attrRecogNode.outputTo(attrSavingNode.createInputPort(
                        DataManagingApp.AttrSavingStream.PED_ATTR_SAVING_PORT));
                reidNode.outputTo(idRankSavingNode.createInputPort(
                        DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_PORT));

                String trackletIdx = (String) param.get(Parameter.TRACKLET_INDEX);
                final GraphDatabaseConnector dbConnector = new FakeDatabaseConnector();
                videoPaths.forEach(path -> {
                    final Tracklet.Identifier id = new Tracklet.Identifier(
                            path.toString(),
                            Integer.valueOf(trackletIdx));
                    final TrackletOrURL url = new TrackletOrURL(dbConnector.getTrackletSavingDir(id.videoID)
                            + "/" + id.serialNumber);
                    final TaskData taskData = new TaskData(
                            Arrays.asList(
                                    attrRecogNode.createInputPort(PedestrianAttrRecogApp.RecogStream.TRACKLET_PORT),
                                    reidNode.createInputPort(PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_PORT)),
                            plan,
                            url);
                    sendWithLog(taskID, taskData, producer, logger);
                });
                break;
            }
            case CommandType.REID_ONLY: {
                // Retrieve track and attr data integrally, then feed them to ReID
                // module.
                ExecutionPlan.Node reidNode = plan.addNode(PedestrianReIDUsingAttrApp.ReIDStream.OUTPUT_TYPE);
                ExecutionPlan.Node idRankSavingNode = plan.addNode(DataManagingApp.IDRankSavingStream.OUTPUT_TYPE);

                reidNode.outputTo(idRankSavingNode.createInputPort(
                        DataManagingApp.IDRankSavingStream.PED_IDRANK_SAVING_PORT));

                String trackletIdx = (String) param.get(Parameter.TRACKLET_INDEX);
                final GraphDatabaseConnector dbConnector = new FakeDatabaseConnector();
                videoPaths.forEach(path -> {
                    final Tracklet.Identifier id = new Tracklet.Identifier(
                            path.toString(),
                            Integer.valueOf(trackletIdx));
                    final TrackletOrURL url = new TrackletOrURL(dbConnector.getTrackletSavingDir(id.videoID)
                            + "/" + id.serialNumber);
                    final Attributes attr;
                    try {
                        attr = new RobustExecutor<Void, Attributes>((Function0<Attributes>) () ->
                                dbConnector.getPedestrianAttributes(id.toString())
                        ).execute();
                    } catch (Exception e) {
                        logger.error("During retrieving attributes", e);
                        return;
                    }
                    final PedestrianInfo info = new PedestrianInfo(url, attr);
                    final TaskData taskData = new TaskData(
                            reidNode.createInputPort(PedestrianReIDUsingAttrApp.ReIDStream.TRACKLET_ATTR_PORT),
                            plan,
                            info);
                    sendWithLog(taskID, taskData, producer, logger);
                });
                break;
            }
            default:
                throw new UnsupportedCommandException();
        }
    }
}
