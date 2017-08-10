<<<<<<< HEAD
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

package org.cripac.isee.vpe.alg.pedestrian.tracking;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.tracking.BasicTracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.ParallelExecutor;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;
import org.cripac.isee.vpe.data.Neo4jConnector;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import kafka.common.FailedToSendMessageException;
import kafka.common.MessageSizeTooLargeException;

/**
 * The PedestrianTrackingApp class takes in video URLs from Kafka, then process
 * the videos with pedestrian tracking algorithms, and finally push the tracking
 * results back to Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianTrackingApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-tracking";
    private static final long serialVersionUID = 662603522385058035L;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianTrackingApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new HDFSVideoTrackingStream(propCenter)));
    }

    /**
     * @param args No options supported currently.
     * @throws Exception Any exception that might occur during execution.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        AppPropertyCenter propCenter = new AppPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianTrackingApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;

        int numSamplesPerTracklet = -1;

        public AppPropertyCenter(@Nonnull String[] args)
                throws SAXException, ParserConfigurationException, URISyntaxException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.num.sample.per.tracklet":
                        numSamplesPerTracklet = Integer.valueOf((String) entry.getValue());
                        break;
                    default:
                        logger.warn("Unrecognized option: " + entry.getKey());
                        break;
                }
            }
        }
    }

    public static class HDFSVideoTrackingStream extends Stream {

        public static final String NAME = "hdfs-video-tracking";
        public static final DataType OUTPUT_TYPE = DataType.TRACKLET;

        /**
         * Port to input video URLs from Kafka.
         */
        public static final Port VIDEO_URL_PORT =
                new Port("hdfs-video-url-for-pedestrian-tracking", DataType.URL);
        private static final long serialVersionUID = -6738652169567844016L;

        private class ConfCache extends HashMap<String, byte[]> {
            private static final long serialVersionUID = -1243878282849738861L;
        }

        private final Singleton<ConfCache> confCacheSingleton;
        private final int numSamplesPerTracklet;
        private final String metadataDir;

//        final GraphDatabaseConnector dbConnector;
        
        public HDFSVideoTrackingStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            numSamplesPerTracklet = propCenter.numSamplesPerTracklet;
            metadataDir = propCenter.metadataDir;
            confCacheSingleton = new Singleton<>(ConfCache::new, ConfCache.class);
            
//            dbConnector = new Neo4jConnector();
        }

        /**
         * Add streaming actions to the global {@link TaskData} stream.
         * This global stream contains pre-deserialized TaskData messages, so as to save time.
         *
         * @param globalStreamMap A map of streams. The key of an entry is the topic name,
         *                        which must be one of the {@link DataType}.
         *                        The value is a filtered stream.
         */
        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            this.filter(globalStreamMap, VIDEO_URL_PORT)
                    .foreachRDD(rdd -> rdd.glom().foreach(kvList -> {
                        final Logger logger = loggerSingleton.getInst();
                        if (kvList.size() > 0) {
                            logger.info("Partition " + TaskContext.getPartitionId()
                                    + " got " + kvList.size()
                                    + " videos in this batch.");
                        }

                        long startTime = System.currentTimeMillis();
                        synchronized (HDFSVideoTrackingStream.class) {
                            ParallelExecutor.execute(kvList, kv -> {
                                try {
                                    final UUID taskID = kv._1();
                                    final TaskData taskData = kv._2();

                                    final String videoURL = (String) taskData.predecessorRes;
                                    logger.debug("Received taskID=" + taskID + ", URL=" + videoURL);
                                    final Path videoPath = new Path(videoURL);
                                    logger.info("源视频路径是："+videoPath.toString().split("8020")[1]);
                                    String videoName = videoPath.getName();
                                    videoName = videoName.substring(0, videoName.lastIndexOf('.'));

                                    // Find current node.
                                    final ExecutionPlan.Node curNode = taskData.getDestNode(VIDEO_URL_PORT);
                                    // Get tracking configuration for this execution.
                                    assert curNode != null;
                                    final String confFile = (String) curNode.getExecData();
                                    if (confFile == null) {
                                        throw new IllegalArgumentException(
                                                "Tracking configuration file is not specified for this node!");
                                    }

                                    // Get ports to output to.
                                    final List<ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                                    // Mark the current node as executed in advance.
                                    curNode.markExecuted();

                                    // Load tracking configuration to create a tracker.
                                    if (!confCacheSingleton.getInst().containsKey(confFile)) {
                                        InputStream confStream = getClass().getResourceAsStream(
                                                "/conf/" + APP_NAME + "/" + confFile);
                                        if (confStream == null) {
                                            throw new IllegalArgumentException(
                                                    "Tracking configuration file not found in JAR!");
                                        }
                                        confCacheSingleton.getInst().put(confFile, IOUtils.toByteArray(confStream));
                                    }
                                    final byte[] confBytes = confCacheSingleton.getInst().get(confFile);
                                    if (confBytes == null) {
                                        logger.fatal("confPool contains key " + confFile + " but value is null!");
                                        return;
                                    }
                                    logger.info("confFile:"+confFile);
                                    
                                    final Tracker tracker = new BasicTracker(confBytes, logger);

                                    final FileSystem hdfs = HDFSFactory.newInstance();

                                    // Conduct tracking on video read from HDFS.
                                    logger.debug("Performing tracking on " + videoName);
                                    final Tracklet[] tracklets = new RobustExecutor<Void, Tracklet[]>(
                                            (Function0<Tracklet[]>) () -> {
                                                // This value is set according to resolution of DCI 4K.
                                                final int BUFFER_SIZE = 4096 * 2160 * 3;
                                                final InputStream videoStream =
                                                        new BufferedInputStream(hdfs.open(videoPath), BUFFER_SIZE);
                                                return tracker.track(videoStream);
                                            }
                                    ).execute();
                                    logger.debug("Finished tracking on " + videoName);

                                    // Set video IDs and Send tracklets.
                                    for (Tracklet tracklet : tracklets) {
                                        // Conduct sampling on the tracklets to save memory.
                                        tracklet.sample(numSamplesPerTracklet);
                                        tracklet.id.videoID = videoName;
                                        try {
                                            output(outputPorts, taskData.executionPlan,
                                                    new TrackletOrURL(tracklet), taskID);
                                        } catch (MessageSizeTooLargeException
                                                | KafkaException
                                                | FailedToSendMessageException e) {
                                            // The tracklet's size exceeds the limit.
                                            // Here we first store it into HDFS,
                                            // then send its URL instead of the tracklet itself.
                                            final String videoRoot = metadataDir + "/new2/" + tracklet.id.videoID;
                                            final String taskRoot = videoRoot + "/" + taskID;
                                            final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                                            hdfs.mkdirs(new Path(storeDir));
                                            logger.debug("Tracklet " + tracklet.id
                                                    + " is too long. Passing it through HDFS at \"" + storeDir + "\".");
                                            logger.info("tracking开始保存图片");
                                            HadoopHelper.storeTracklet(tracklet.id.videoID,storeDir, tracklet, hdfs
//                                            		,dbConnector
                                            		,logger
                                            		);
                                            
                                            logger.info("tracking保存图片结束");
                                            output(outputPorts,
                                                    taskData.executionPlan,
                                                    new TrackletOrURL(storeDir),
                                                    taskID);
                                        }
                                    }

                                    hdfs.close();
                                } catch (Throwable e) {
                                    logger.error("During tracking.", e);
                                }
                            });
                        }
                        if (kvList.size() > 0) {
                            long endTime = System.currentTimeMillis();
                            logger.info("Average cost time: " + ((endTime - startTime) / kvList.size()) + "ms");
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(VIDEO_URL_PORT);
        }

    }

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }
}
=======
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

package org.cripac.isee.vpe.alg.pedestrian.tracking;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.tracking.BasicTracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.ParallelExecutor;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;
import org.cripac.isee.vpe.data.Neo4jConnector;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import kafka.common.FailedToSendMessageException;
import kafka.common.MessageSizeTooLargeException;

/**
 * The PedestrianTrackingApp class takes in video URLs from Kafka, then process
 * the videos with pedestrian tracking algorithms, and finally push the tracking
 * results back to Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianTrackingApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-tracking";
    private static final long serialVersionUID = 662603522385058035L;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianTrackingApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new HDFSVideoTrackingStream(propCenter)));
    }

    /**
     * @param args No options supported currently.
     * @throws Exception Any exception that might occur during execution.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        AppPropertyCenter propCenter = new AppPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianTrackingApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;

        int numSamplesPerTracklet = -1;

        public AppPropertyCenter(@Nonnull String[] args)
                throws SAXException, ParserConfigurationException, URISyntaxException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.num.sample.per.tracklet":
                        numSamplesPerTracklet = Integer.valueOf((String) entry.getValue());
                        break;
                    default:
                        logger.warn("Unrecognized option: " + entry.getKey());
                        break;
                }
            }
        }
    }

    public static class HDFSVideoTrackingStream extends Stream {

        public static final String NAME = "hdfs-video-tracking";
        public static final DataType OUTPUT_TYPE = DataType.TRACKLET;

        /**
         * Port to input video URLs from Kafka.
         */
        public static final Port VIDEO_URL_PORT =
                new Port("hdfs-video-url-for-pedestrian-tracking", DataType.URL);
        private static final long serialVersionUID = -6738652169567844016L;

        private class ConfCache extends HashMap<String, byte[]> {
            private static final long serialVersionUID = -1243878282849738861L;
        }

        private final Singleton<ConfCache> confCacheSingleton;
        private final int numSamplesPerTracklet;
        private final String metadataDir;

//        final GraphDatabaseConnector dbConnector;
        
        public HDFSVideoTrackingStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            numSamplesPerTracklet = propCenter.numSamplesPerTracklet;
            metadataDir = propCenter.metadataDir;
            confCacheSingleton = new Singleton<>(ConfCache::new, ConfCache.class);
            
//            dbConnector = new Neo4jConnector();
        }

        /**
         * Add streaming actions to the global {@link TaskData} stream.
         * This global stream contains pre-deserialized TaskData messages, so as to save time.
         *
         * @param globalStreamMap A map of streams. The key of an entry is the topic name,
         *                        which must be one of the {@link DataType}.
         *                        The value is a filtered stream.
         */
        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            this.filter(globalStreamMap, VIDEO_URL_PORT)
                    .foreachRDD(rdd -> rdd.glom().foreach(kvList -> {
                        final Logger logger = loggerSingleton.getInst();
                        if (kvList.size() > 0) {
                            logger.info("Partition " + TaskContext.getPartitionId()
                                    + " got " + kvList.size()
                                    + " videos in this batch.");
                        }

                        long startTime = System.currentTimeMillis();
                        synchronized (HDFSVideoTrackingStream.class) {
                            ParallelExecutor.execute(kvList, kv -> {
                                try {
                                    final UUID taskID = kv._1();
                                    final TaskData taskData = kv._2();

                                    final String videoURL = (String) taskData.predecessorRes;
                                    logger.debug("Received taskID=" + taskID + ", URL=" + videoURL);
                                    final Path videoPath = new Path(videoURL);
                                    logger.info("源视频路径是："+videoPath.toString().split("8020")[1]);
                                    String videoName = videoPath.getName();
                                    videoName = videoName.substring(0, videoName.lastIndexOf('.'));

                                    // Find current node.
                                    final ExecutionPlan.Node curNode = taskData.getDestNode(VIDEO_URL_PORT);
                                    // Get tracking configuration for this execution.
                                    assert curNode != null;
                                    final String confFile = (String) curNode.getExecData();
                                    if (confFile == null) {
                                        throw new IllegalArgumentException(
                                                "Tracking configuration file is not specified for this node!");
                                    }

                                    // Get ports to output to.
                                    final List<ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                                    // Mark the current node as executed in advance.
                                    curNode.markExecuted();

                                    // Load tracking configuration to create a tracker.
                                    if (!confCacheSingleton.getInst().containsKey(confFile)) {
                                        InputStream confStream = getClass().getResourceAsStream(
                                                "/conf/" + APP_NAME + "/" + confFile);
                                        if (confStream == null) {
                                            throw new IllegalArgumentException(
                                                    "Tracking configuration file not found in JAR!");
                                        }
                                        confCacheSingleton.getInst().put(confFile, IOUtils.toByteArray(confStream));
                                    }
                                    final byte[] confBytes = confCacheSingleton.getInst().get(confFile);
                                    if (confBytes == null) {
                                        logger.fatal("confPool contains key " + confFile + " but value is null!");
                                        return;
                                    }
                                    final Tracker tracker = new BasicTracker(confBytes, logger);

                                    final FileSystem hdfs = HDFSFactory.newInstance();

                                    // Conduct tracking on video read from HDFS.
                                    logger.debug("Performing tracking on " + videoName);
                                    final Tracklet[] tracklets = new RobustExecutor<Void, Tracklet[]>(
                                            (Function0<Tracklet[]>) () -> {
                                                // This value is set according to resolution of DCI 4K.
                                                final int BUFFER_SIZE = 4096 * 2160 * 3;
                                                final InputStream videoStream =
                                                        new BufferedInputStream(hdfs.open(videoPath), BUFFER_SIZE);
                                                return tracker.track(videoStream);
                                            }
                                    ).execute();
                                    logger.debug("Finished tracking on " + videoName);

                                    // Set video IDs and Send tracklets.
                                    for (Tracklet tracklet : tracklets) {
                                        // Conduct sampling on the tracklets to save memory.
                                        tracklet.sample(numSamplesPerTracklet);
                                        tracklet.id.videoID = videoName;
                                        try {
                                            output(outputPorts, taskData.executionPlan,
                                                    new TrackletOrURL(tracklet), taskID);
                                        } catch (MessageSizeTooLargeException
                                                | KafkaException
                                                | FailedToSendMessageException e) {
                                            // The tracklet's size exceeds the limit.
                                            // Here we first store it into HDFS,
                                            // then send its URL instead of the tracklet itself.
                                            final String videoRoot = metadataDir + "/new2/" + tracklet.id.videoID;
                                            final String taskRoot = videoRoot + "/" + taskID;
                                            final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                                            hdfs.mkdirs(new Path(storeDir));
                                            logger.debug("Tracklet " + tracklet.id
                                                    + " is too long. Passing it through HDFS at \"" + storeDir + "\".");
                                            logger.info("tracking开始保存图片");
                                            HadoopHelper.storeTracklet(tracklet.id.videoID,storeDir, tracklet, hdfs
//                                            		,dbConnector
                                            		,logger
                                            		);
                                            
                                            logger.info("tracking保存图片结束");
                                            output(outputPorts,
                                                    taskData.executionPlan,
                                                    new TrackletOrURL(storeDir),
                                                    taskID);
                                        }
                                    }

                                    hdfs.close();
                                } catch (Throwable e) {
                                    logger.error("During tracking.", e);
                                }
                            });
                        }
                        if (kvList.size() > 0) {
                            long endTime = System.currentTimeMillis();
                            logger.info("Average cost time: " + ((endTime - startTime) / kvList.size()) + "ms");
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(VIDEO_URL_PORT);
        }

    }

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }
}
>>>>>>> branch 'win_eclipse' of https://github.com/kyu-sz/LaS-VPE-Platform.git
