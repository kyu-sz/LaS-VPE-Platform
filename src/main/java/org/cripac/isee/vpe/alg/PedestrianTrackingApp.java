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

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.pedestrian.tracking.BasicTracker;
import org.cripac.isee.pedestrian.tracking.Tracker;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.data.WebCameraConnector;
import org.cripac.isee.vpe.debug.FakeWebCameraConnector;
import org.cripac.isee.vpe.util.SerializationHelper;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The PedestrianTrackingApp class takes in video URLs from Kafka, then process
 * the videos with pedestrian tracking algorithms, and finally push the tracking
 * results back to Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianTrackingApp extends SparkStreamingApp {
    /**
     * The NAME of this application.
     */
    public static final String APP_NAME = "PedestrianTracking";

    private Stream fragmentTrackingStream;
    private Stream rtTrackingStream;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianTrackingApp(SystemPropertyCenter propCenter) throws Exception {
        fragmentTrackingStream = new VideoFragmentTrackingStream(propCenter);
        rtTrackingStream = new RTVideoStreamTrackingStream(propCenter);
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
        SparkStreamingApp app = new PedestrianTrackingApp(propCenter);
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
        JavaStreamingContext jsc =
                new JavaStreamingContext(new SparkConf(true), Durations.seconds(2));

        fragmentTrackingStream.addToContext(jsc);
        rtTrackingStream.addToContext(jsc);

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
     * The class ConfigPool wraps a broadcast of a pool of bytes of
     * tracking configuration files.
     */
    private static class ConfigPool {

        private static volatile Broadcast<Map<String, byte[]>> inst = null;

        /**
         * Get an instance of the pool broadcast.
         *
         * @param jsc    The JavaSparkContext the driver is running on.
         * @param hdfs   HDFS to read files from.
         * @param logger Logger.
         * @return An instance of the pool broadcast.
         * @throws IOException On failure of accessing HDFS for uploaded files.
         */
        public static Broadcast<Map<String, byte[]>> getInst(JavaSparkContext jsc,
                                                             FileSystem hdfs,
                                                             Logger logger) throws IOException {
            if (inst == null) {
                logger.debug("Creating instance of ConfigPool...");
                Map<String, byte[]> pool = new HashedMap();
                FileSystem stagingDir = FileSystem.get(new Configuration());
                RemoteIterator<LocatedFileStatus> files =
                        stagingDir.listFiles(new Path(System.getenv("SPARK_YARN_STAGING_DIR")), false);
                while (files.hasNext()) {
                    Path path = files.next().getPath();
                    if (path.getName().contains(".conf")) {
                        try {
                            logger.debug("Reading " + path.getName() + "...");
                            pool.put(path.getName(), IOUtils.toByteArray(hdfs.open(path)));
                            logger.debug("Added " + path.getName() + " to tracking configuration pool.");
                        } catch (IOException e) {
                            logger.error("Error when reading file " + path.getName(), e);
                        }
                    }
                }
                inst = jsc.broadcast(pool);
            }
            return inst;
        }
    }

    /**
     * The RTVideoStreamTrackingStream receives web-camera IPs
     * and perform pedestrian tracking on real-time video bit
     * stream from these cameras.
     */
    public static class RTVideoStreamTrackingStream extends Stream {

        public static final Info INFO =
                new Info("RTVideoStreamPedestrianTracking", DataType.TRACKLET);

        /**
         * Topic for inputting from Kafka the IPs of cameras.
         */
        public static final Topic LOGIN_PARAM_TOPIC =
                new Topic("cam-ip-for-pedestrian-tracking",
                        DataType.WEBCAM_LOGIN_PARAM, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages
         * from Kafka brokers.
         */
        private Map<String, String> kafkaParams = new HashMap<>();

        /**
         * Topics for inputting camera IPs. Each assigned a number of
         * threads the Kafka consumer should use.
         */
        private Map<String, Integer> camIPTopicMap = new HashMap<>();

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<SynthesizedLogger> loggerSingleton;
        private Singleton<FileSystem> hdfsSingleton;
        private Map<ServerID, Singleton<WebCameraConnector>> connectorPool;

        public RTVideoStreamTrackingStream(SystemPropertyCenter propCenter) throws
                Exception {
            camIPTopicMap.put(LOGIN_PARAM_TOPIC.NAME,
                    propCenter.kafkaNumPartitions);

            kafkaParams.put("metadata.broker.list", propCenter.kafkaBrokers);
            kafkaParams.put("group.id",
                    "PedestrianTrackingApp" + UUID.randomUUID());
            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            // Determine where the stream starts (default: largest)
            kafkaParams.put("auto.offset.reset", "smallest");
            kafkaParams.put("fetch.message.max.bytes",
                    "" + propCenter.kafkaFetchMsgMaxBytes);

            Properties producerProp = new Properties();
            producerProp.put("bootstrap.servers", propCenter.kafkaBrokers);
            producerProp.put("compression.codec", "1");
            producerProp.put("max.request.size", "10000000");
            producerProp.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProp.put("value.serializer",
                    "org.apache.kafka.common.serialization.ByteArraySerializer");

            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));
            loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(
                    INFO.NAME,
                    propCenter.verbose ? Level.DEBUG : Level.INFO,
                    propCenter.reportListenerAddr,
                    propCenter.reportListenerPort));
            hdfsSingleton = new Singleton<>(new HDFSFactory());
            connectorPool = new HashedMap();
        }

        @Override
        public void addToContext(JavaStreamingContext jsc) {
            buildBytesDirectStream(jsc, kafkaParams, camIPTopicMap)
                    .foreachRDD(rdd -> {
                        rdd.foreach(kvPair -> {
                            // Recover data.
                            final String taskID = kvPair._1();
                            TaskData taskData = (TaskData) SerializationHelper.deserialize(kvPair._2());

                            // Get camera WEBCAM_LOGIN_PARAM.
                            if (taskData.predecessorRes == null) {
                                loggerSingleton.getInst().error(
                                        "No camera WEBCAM_LOGIN_PARAM specified for real-time tracking stream!");
                                return;
                            }
                            if (!(taskData.predecessorRes instanceof String)) {
                                loggerSingleton.getInst().error(
                                        "Real-time tracking stream expects camera WEBCAM_LOGIN_PARAM but received "
                                                + taskData.predecessorRes.getClass().getName() + "!");
                                return;
                            }
                            WebCameraConnector.LoginParam loginParam =
                                    (WebCameraConnector.LoginParam) taskData.predecessorRes;

                            WebCameraConnector cameraConnector;
                            if (connectorPool.containsKey(loginParam.camID)) {
                                cameraConnector = connectorPool.get(loginParam.camID).getInst();
                            } else {
                                Singleton<WebCameraConnector> cameraConnectorSingleton =
                                        new Singleton(
                                                new FakeWebCameraConnector
                                                        .FakeWebCameraConnectorFactory(loginParam));
                                connectorPool.put(loginParam.camID, cameraConnectorSingleton);
                                cameraConnector = cameraConnectorSingleton.getInst();
                            }

                            // Connect to camera.
                            InputStream rtVideoStream = cameraConnector.getStream();
                            // TODO(Ken Yu): Perform tracking on the real-time video stream.
                        });
                    });
        }
    }

    public static class VideoFragmentTrackingStream extends Stream {

        public static final Info INFO =
                new Info("VideoFragmentPedestrianTracking", DataType.TRACKLET);

        /**
         * Topic to input video URLs from Kafka.
         */
        public static final Topic VIDEO_URL_TOPIC =
                new Topic("video-url-for-pedestrian-tracking",
                        DataType.URL, INFO);
        /**
         * Topic to input video bytes from Kafka.
         */
        public static final Topic VIDEO_FRAG_BYTES_TOPIC =
                new Topic("video-fragment-bytes-for-pedestrian-tracking",
                        DataType.RAW_VIDEO_FRAG_BYTES, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages
         * from Kafka brokers.
         */
        private Map<String, String> kafkaParams = new HashMap<>();

        /**
         * Topics for inputting video URLs. Each assigned a number of
         * threads the Kafka consumer should use.
         */
        private Map<String, Integer> videoURLTopicMap = new HashMap<>();
        /**
         * Topics for inputting video bytes. Each assigned a number of
         * threads the Kafka consumer should use.
         */
        private Map<String, Integer> videoFragBytesTopicMap = new HashMap<>();

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<SynthesizedLogger> loggerSingleton;
        private Singleton<FileSystem> hdfsSingleton;

        public VideoFragmentTrackingStream(SystemPropertyCenter propCenter) throws
                Exception {
            videoURLTopicMap.put(VIDEO_URL_TOPIC.NAME,
                    propCenter.kafkaNumPartitions);
            videoFragBytesTopicMap.put(VIDEO_FRAG_BYTES_TOPIC.NAME,
                    propCenter.kafkaNumPartitions);

            kafkaParams.put("metadata.broker.list", propCenter.kafkaBrokers);
            kafkaParams.put("group.id",
                    "PedestrianTrackingApp" + UUID.randomUUID());
            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            // Determine where the stream starts (default: largest)
            kafkaParams.put("auto.offset.reset", "smallest");
            kafkaParams.put("fetch.message.max.bytes",
                    "" + propCenter.kafkaFetchMsgMaxBytes);

            Properties producerProp = new Properties();
            producerProp.put("bootstrap.servers", propCenter.kafkaBrokers);
            producerProp.put("compression.codec", "1");
            producerProp.put("max.request.size", "10000000");
            producerProp.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProp.put("value.serializer",
                    "org.apache.kafka.common.serialization.ByteArraySerializer");

            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));
            loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(
                    INFO.NAME,
                    propCenter.verbose ? Level.DEBUG : Level.INFO,
                    propCenter.reportListenerAddr,
                    propCenter.reportListenerPort));
            hdfsSingleton = new Singleton<>(new HDFSFactory());
        }

        public static class VideoFragment implements Serializable {
            public String videoID;
            public byte[] bytes;
        }

        @Override
        public void addToContext(JavaStreamingContext jsc) {
            JavaPairDStream<String, TaskData> fragFromURLDStream =
                    buildBytesDirectStream(jsc, kafkaParams, videoURLTopicMap)
                            .mapToPair(kvPair -> {
                                String taskID = kvPair._1();

                                // Get the task data.
                                TaskData taskData =
                                        (TaskData) deserialize(kvPair._2());
                                VideoFragment frag = new VideoFragment();
                                // Get the videoID of the video to process from the
                                // execution data of this node.
                                frag.videoID = (String) taskData.predecessorRes;
                                // Retrieve video fragment bytes.
                                frag.bytes = IOUtils.toByteArray(
                                        hdfsSingleton.getInst().open(new Path(frag.videoID)));

                                taskData.predecessorRes = frag;
                                return new Tuple2<>(taskID, taskData);
                            });

            JavaPairDStream<String, TaskData> fragFromBytesDStream =
                    buildBytesDirectStream(jsc, kafkaParams, videoFragBytesTopicMap)
                            .mapValues(bytes -> (TaskData) SerializationHelper.deserialize(bytes));

            fragFromURLDStream.union(fragFromBytesDStream).foreachRDD(rdd -> {
                final Broadcast<Map<String, byte[]>> cfgPool =
                        ConfigPool.getInst(
                                new JavaSparkContext(rdd.context()),
                                hdfsSingleton.getInst(),
                                loggerSingleton.getInst());

                rdd.foreach(task -> {
                    SynthesizedLogger logger = loggerSingleton.getInst();

                    // Get the task data.
                    TaskData taskData = task._2();
                    TaskData.ExecutionPlan.Node curNode =
                            taskData.curNode;
                    // Get the videoID of the video to process from the
                    // execution data of this node.
                    VideoFragment frag = (VideoFragment) taskData.predecessorRes;
                    // Get tracking configuration for this execution.
                    String cfgFile = (String) curNode.getExecData();
                    if (cfgFile == null) {
                        logger.error("Tracking configuration file" +
                                " is not specified for this node!");
                        return;
                    }

                    // Get the IDs of successor nodes.
                    List<Topic> succTopics = curNode.getSuccessors();
                    // Mark the current node as executed in advance.
                    taskData.curNode.markExecuted();

                    // Load tracking configuration to create a tracker.
                    if (!cfgPool.getValue().containsKey(cfgFile)) {
                        logger.error(
                                "Cannot find tracking config file "
                                        + cfgFile);
                        return;
                    }
                    byte[] confBytes = cfgPool.getValue().get(cfgFile);
                    if (confBytes == null) {
                        logger.fatal("cfgPool contains key " + cfgFile
                                + " but value is null!");
                        return;
                    }
                    Tracker tracker = new BasicTracker(confBytes, logger);
                    //Tracker tracker = new FakePedestrianTracker();

                    // Conduct tracking on video read from HDFS.
                    logger.debug("Performing tracking on " + frag.videoID);
                    Tracklet[] tracklets = tracker.track(frag.bytes);
                    logger.debug("Finished tracking on " + frag.videoID);

                    // Send tracklets.
                    for (int i = 0; i < tracklets.length; ++i) {
                        Tracklet tracklet = tracklets[i];
                        // Complete identifier of each tracklet.
                        tracklet.id = new Tracklet.Identifier(frag.videoID, i);
                        // Stored the track in the task data, which can be cyclic utilized.
                        taskData.predecessorRes = tracklet;
                        // Send to all the successor nodes.
                        for (Topic topic : succTopics) {
                            taskData.changeCurNode(topic);

                            byte[] serialized = serialize(taskData);
                            logger.debug(
                                    "To sendWithLog message with size: "
                                            + serialized.length);
                            sendWithLog(topic,
                                    task._1(),
                                    serialized,
                                    producerSingleton.getInst(),
                                    logger);
                        }
                    }
                });
            });
        }
    }
}
