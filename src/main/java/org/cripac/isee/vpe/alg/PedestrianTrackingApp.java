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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.Tuple2;

import java.io.FileNotFoundException;
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
    public static final String APP_NAME = "pedestrian-tracking";
    private int batchDuration = 1000;

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
        batchDuration = propCenter.batchDuration;
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
                new JavaStreamingContext(new SparkConf(true), Durations.seconds(batchDuration));

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
                new Info("rt-video-tracking", DataTypes.TRACKLET);

        /**
         * Topic for inputting from Kafka the IPs of cameras.
         */
        public static final Topic LOGIN_PARAM_TOPIC =
                new Topic("cam-address-for-pedestrian-tracking",
                        DataTypes.WEBCAM_LOGIN_PARAM, INFO);

        private final int procTime;

        /**
         * Kafka parameters for creating input streams pulling messages
         * from Kafka brokers.
         */
        private Map<String, String> kafkaParams = new HashMap<>();

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<FileSystem> hdfsSingleton;
        private Map<ServerID, Singleton<WebCameraConnector>> connectorPool;

        public RTVideoStreamTrackingStream(SystemPropertyCenter propCenter) throws
                Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            this.procTime = propCenter.procTime;

            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    propCenter.kafkaBootstrapServers);
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,
                    INFO.NAME);
//            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            // Determine where the stream starts (default: largest)
            kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
            kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put("fetch.message.max.bytes",
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.SEND_BUFFER_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);

            Properties producerProp = new Properties();
            producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    propCenter.kafkaBootstrapServers);
            producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    propCenter.kafkaMaxRequestSize);
            producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
            producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);

            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));
            hdfsSingleton = new Singleton<>(new HDFSFactory());
            connectorPool = new HashedMap();
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            buildBytesDirectStream(jssc, Arrays.asList(LOGIN_PARAM_TOPIC.NAME), kafkaParams, procTime)
                    .foreachRDD(rdd ->
                        rdd.foreach(kvPair -> {
                            // Recover data.
                            final String taskID = kvPair._1();
                            TaskData taskData;
                            try {
                                taskData = (TaskData) deserialize(kvPair._2());
                            } catch (Exception e) {
                                loggerSingleton.getInst().error("During TaskData deserialization", e);
                                return;
                            }

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
                            LoginParam loginParam =
                                    (LoginParam) taskData.predecessorRes;

                            WebCameraConnector cameraConnector;
                            if (connectorPool.containsKey(loginParam.serverID)) {
                                cameraConnector = connectorPool.get(loginParam.serverID).getInst();
                            } else {
                                Singleton<WebCameraConnector> cameraConnectorSingleton =
                                        new Singleton(new FakeWebCameraConnector
                                                .FakeWebCameraConnectorFactory(loginParam));
                                connectorPool.put(loginParam.serverID, cameraConnectorSingleton);
                                cameraConnector = cameraConnectorSingleton.getInst();
                            }

                            // Connect to camera.
                            InputStream rtVideoStream = cameraConnector.getStream();
                            // TODO(Ken Yu): Perform tracking on the real-time video stream.
                        })
                    );
        }
    }

    public static class VideoFragmentTrackingStream extends Stream {

        public static final Info INFO =
                new Info("video-frag-tracking", DataTypes.TRACKLET);

        /**
         * Topic to input video URLs from Kafka.
         */
        public static final Topic VIDEO_URL_TOPIC =
                new Topic("video-url-for-pedestrian-tracking",
                        DataTypes.URL, INFO);
        /**
         * Topic to input video bytes from Kafka.
         */
        public static final Topic VIDEO_FRAG_BYTES_TOPIC =
                new Topic("video-fragment-bytes-for-pedestrian-tracking",
                        DataTypes.RAW_VIDEO_FRAG_BYTES, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages
         * from Kafka brokers.
         */
        private Map<String, String> kafkaParams = new HashMap<>();

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<FileSystem> hdfsSingleton;
        private final int procTime;

        public VideoFragmentTrackingStream(SystemPropertyCenter propCenter) throws
                Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            this.procTime = propCenter.procTime;

            kafkaParams = propCenter.generateKafkaParams(INFO.NAME);

            Properties producerProp = propCenter.generateKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));

            hdfsSingleton = new Singleton<>(new HDFSFactory());
        }

        public static class VideoFragment implements Serializable {
            public String videoID;
            public byte[] bytes;
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            JavaPairDStream<String, TaskData> fragFromURLDStream =
                    buildBytesDirectStream(jssc, Arrays.asList(VIDEO_URL_TOPIC.NAME), kafkaParams, procTime)
                            .mapToPair(kvPair -> {
                                String taskID = kvPair._1();

                                // Get the task data.
                                TaskData taskData;
                                try {
                                    taskData = (TaskData) deserialize(kvPair._2());
                                } catch (Exception e) {
                                    loggerSingleton.getInst().error("During TaskData deserialization", e);
                                    return null;
                                }
                                VideoFragment frag = new VideoFragment();
                                // Get the videoID of the video to process from
                                // the execution data of this node. Here, the
                                // ID is represented by the URL of the video.
                                frag.videoID = (String) taskData.predecessorRes;
                                // Retrieve video fragment bytes.
                                frag.bytes = IOUtils.toByteArray(hdfsSingleton.getInst().open(new Path(frag.videoID)));

                                loggerSingleton.getInst().debug("Received taskID=" + taskID + ", URL=" + frag.videoID);

                                taskData.predecessorRes = frag;
                                return new Tuple2<>(taskID, taskData);
                            });

            JavaPairDStream<String, TaskData> fragFromBytesDStream =
                    buildBytesDirectStream(jssc, Arrays.asList(VIDEO_FRAG_BYTES_TOPIC.NAME), kafkaParams, procTime)
                            .mapValues(bytes -> {
                                TaskData taskData;
                                try {
                                    taskData = (TaskData) deserialize(bytes);
                                    return taskData;
                                } catch (Exception e) {
                                    loggerSingleton.getInst().error("During TaskData deserialization", e);
                                    return null;
                                }
                            });

            JavaPairDStream<String, TaskData> fragUnionStream = fragFromURLDStream.union(fragFromBytesDStream);

            fragUnionStream.foreachRDD(rdd -> {
                final Broadcast<Map<String, byte[]>> confPool =
                        ConfigPool.getInst(new JavaSparkContext(rdd.context()),
                                hdfsSingleton.getInst(),
                                loggerSingleton.getInst());

                rdd.foreach(task -> {
                    try {
                        Logger logger = loggerSingleton.getInst();

                        // Get the task data.
                        TaskData taskData = task._2();
                        TaskData.ExecutionPlan.Node curNode = taskData.curNode;
                        // Get the videoID of the video to process from the
                        // execution data of this node.
                        VideoFragment frag = (VideoFragment) taskData.predecessorRes;
                        // Get tracking configuration for this execution.
                        String confFile = (String) curNode.getExecData();
                        if (confFile == null) {
                            logger.error("Tracking configuration file is not specified for this node!");
                            return;
                        }

                        // Get the IDs of successor nodes.
                        List<Topic> succTopics = curNode.getSuccessors();
                        // Mark the current node as executed in advance.
                        taskData.curNode.markExecuted();

                        // Load tracking configuration to create a tracker.
                        if (!confPool.getValue().containsKey(confFile)) {
                            throw new FileNotFoundException("Cannot find tracking config file " + confFile);
                        }
                        byte[] confBytes = confPool.getValue().get(confFile);
                        if (confBytes == null) {
                            logger.fatal("confPool contains key " + confFile + " but value is null!");
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
                                try {
                                    taskData.changeCurNode(topic);
                                } catch (RecordNotFoundException e) {
                                    logger.warn("When changing node in TaskData", e);
                                }

                                byte[] serialized = serialize(taskData);
                                logger.debug("To sendWithLog message with size: "
                                        + serialized.length);
                                sendWithLog(topic,
                                        task._1(),
                                        serialized,
                                        producerSingleton.getInst(),
                                        logger);
                            }
                        }
                    } catch (Exception e) {
                        loggerSingleton.getInst().error("During tracking.", e);
                    }
                });
            });
        }
    }
}
