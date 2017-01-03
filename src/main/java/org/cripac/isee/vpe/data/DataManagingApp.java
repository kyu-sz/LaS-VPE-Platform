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

package org.cripac.isee.vpe.data;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.helper.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_imgproc;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;
import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.retrieveTracklet;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The DataManagingApp class combines two functions: meta data saving and data
 * feeding. The meta data saving function saves meta data, which may be the
 * results of vision algorithms, to HDFS and Neo4j database. The data feeding
 * function retrieves stored results and sendWithLog them to algorithm modules from
 * HDFS and Neo4j database. The reason why combine these two functions is that
 * they should both be modified when and only when a new data INPUT_TYPE shall be
 * supported by the system, and they require less resources than other modules,
 * so combining them can save resources while not harming performance.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class DataManagingApp extends SparkStreamingApp {
    /**
     * The NAME of this application.
     */
    public static final String APP_NAME = "data-managing";

    private Stream pedTrackletRtrvStream;
    private Stream pedTrackletAttrRtrvStream;
    private Stream savingStream;

    public DataManagingApp(SystemPropertyCenter propCenter) throws Exception {
        pedTrackletRtrvStream = new PedestrainTrackletRetrievingStream(propCenter);
        pedTrackletAttrRtrvStream = new PedestrainTrackletAttrRetrievingStream(propCenter);
        savingStream = new SavingStream(propCenter);
    }

    public static void main(String[] args) throws Exception {
        SystemPropertyCenter propCenter;
        if (args.length > 0) {
            propCenter = new SystemPropertyCenter(args);
        } else {
            propCenter = new SystemPropertyCenter();
        }

        SparkStreamingApp app = new DataManagingApp(propCenter);
        TopicManager.checkTopics(propCenter);
        app.initialize(propCenter);
        app.start();
        app.awaitTermination();
    }

    @Override
    protected JavaStreamingContext getStreamContext() {
        // Create contexts.
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf(true));
        JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.seconds(2));

        // Setup streams for data feeding.
        pedTrackletRtrvStream.addToContext(jsc);
        pedTrackletAttrRtrvStream.addToContext(jsc);

        // Setup streams for meta data saving.
        savingStream.addToContext(jsc);

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

    public static class PedestrainTrackletRetrievingStream extends Stream {

        public static final Info INFO = new Info(
                "pedestrian-tracklet-rtrv",
                DataTypes.TRACKLET);
        public static final Topic RTRV_JOB_TOPIC =
                new Topic("pedestrian-tracklet-rtrv-job", DataTypes.TRACKLET_ID, INFO);
        private Map<String, String> kafkaParams = new HashMap<>();
        // Create KafkaSink for Spark Streaming to output to Kafka.
        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<GraphDatabaseConnector> dbConnSingleton;
        private final int procTime;

        public PedestrainTrackletRetrievingStream(SystemPropertyCenter propCenter)
                throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            this.procTime = propCenter.procTime;

            // Common Kafka settings
            kafkaParams = propCenter.generateKafkaParams(INFO.NAME);

            Properties producerProp = propCenter.generateKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));

            dbConnSingleton = new Singleton<>(() -> new FakeDatabaseConnector());
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            // Read track retrieving jobs in parallel from Kafka.
            // URL of a video is given.
            // The directory storing the tracklets of the video is stored in the database.
            buildBytesDirectStream(jssc, Arrays.asList(RTRV_JOB_TOPIC.NAME), kafkaParams, procTime)
                    // Retrieve and deliver tracklets.
                    .foreachRDD(rdd -> {
                        rdd.foreach(job -> {
                            Logger logger = loggerSingleton.getInst();

                            // Recover task data.
                            TaskData taskData;
                            try {
                                taskData = (TaskData) deserialize(job._2());
                            } catch (Exception e) {
                                logger.error("During TaskData deserialization", e);
                                return;
                            }
                            if (taskData.predecessorRes == null) {
                                logger.fatal("TaskData from " + taskData.predecessorInfo
                                        + " contains no result data!");
                                return;
                            }
                            if (!(taskData.predecessorRes instanceof Tracklet.Identifier)) {
                                logger.fatal("TaskData from " + taskData.predecessorInfo
                                        + " contains no result data!");
                                logger.fatal("Result sent by "
                                        + taskData.predecessorInfo
                                        + " is expected to be a tracklet identifier,"
                                        + " but received \""
                                        + taskData.predecessorRes + "\"!");
                                return;
                            }
                            Tracklet.Identifier trackletID =
                                    (Tracklet.Identifier) taskData.predecessorRes;
                            // Retrieve the track from HDFS.
                            Tracklet tracklet = retrieveTracklet(
                                    dbConnSingleton.getInst().getTrackletSavingDir(
                                            trackletID.videoID),
                                    trackletID,
                                    loggerSingleton.getInst());
                            // Store the track to a task data (reused).
                            taskData.predecessorRes = tracklet;

                            // Get the IDs of successor nodes.
                            List<Topic> succTopics = taskData.curNode.getSuccessors();
                            // Mark the current node as executed.
                            taskData.curNode.markExecuted();

                            if (succTopics.size() == 0) {
                                loggerSingleton.getInst().debug(
                                        "No succeeding topics found for"
                                                + " pedestrian tracklet retrieving stream!");
                            }
                            // Send to all the successor nodes.
                            for (Topic topic : succTopics) {
                                try {
                                    taskData.changeCurNode(topic);
                                } catch (RecordNotFoundException e) {
                                    logger.warn("When changing node in TaskData", e);
                                }
                                sendWithLog(topic,
                                        job._1(),
                                        serialize(taskData),
                                        producerSingleton.getInst(),
                                        loggerSingleton.getInst());
                            }
                        });
                    });
        }
    }

    public static class PedestrainTrackletAttrRetrievingStream extends Stream {

        public static final Info INFO = new Info(
                "pedestrian-tracklet-attr-rtrv",
                DataTypes.TRACKLET_ATTR);
        public static final Topic RTRV_JOB_TOPIC =
                new Topic("pedestrian-tracklet-attr-rtrv-job", DataTypes.TRACKLET_ID, INFO);
        private Map<String, String> kafkaParams = new HashMap<>();
        // Create KafkaSink for Spark Streaming to output to Kafka.
        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<GraphDatabaseConnector> dbConnSingleton;
        private final int procTime;

        public PedestrainTrackletAttrRetrievingStream(SystemPropertyCenter propCenter) throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            this.procTime = propCenter.procTime;

            // Common Kafka settings
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,
                    INFO.NAME);
//            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    propCenter.kafkaBootstrapServers);
            kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "largest");
            kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put("fetch.message.max.bytes",
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());
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

            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(
                    producerProp));
            dbConnSingleton = new Singleton<>(() -> new FakeDatabaseConnector());
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            // Read track with attributes retrieving jobs in parallel from Kafka.
            buildBytesDirectStream(jssc, Arrays.asList(RTRV_JOB_TOPIC.NAME), kafkaParams, procTime)
                    // Retrieve and deliver tracklets with attributes.
                    .foreachRDD(rdd -> rdd.foreach(job -> {
                        Logger logger = loggerSingleton.getInst();
                        // Recover task data.
                        TaskData taskData;
                        try {
                            taskData = (TaskData) deserialize(job._2());
                        } catch (Exception e) {
                            logger.error("During TaskData deserialization", e);
                            return;
                        }
                        // Get parameters for the job.
                        Tracklet.Identifier trackletID =
                                (Tracklet.Identifier) taskData.predecessorRes;
                        String videoURL = trackletID.videoID;

                        PedestrianInfo info = new PedestrianInfo();
                        // Retrieve the track from HDFS.
                        info.tracklet = retrieveTracklet(
                                dbConnSingleton.getInst().getTrackletSavingDir(videoURL),
                                trackletID,
                                logger);
                        // Retrieve the attributes from database.
                        info.attr = dbConnSingleton.getInst().getPedestrianAttributes(trackletID.toString());
                        taskData.predecessorRes = info;

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
                                    job._1(),
                                    serialize(taskData),
                                    producerSingleton.getInst(),
                                    logger);
                        }
                    }));
        }
    }

    public static class SavingStream extends Stream {

        public static final Info INFO = new Info("data-saving", DataTypes.NONE);
        public static final Topic PED_TRACKLET_SAVING_TOPIC =
                new Topic("pedestrian-tracklet-saving",
                        DataTypes.TRACKLET,
                        SavingStream.INFO);
        public static final Topic PED_ATTR_SAVING_TOPIC =
                new Topic("pedestrian-attr-saving",
                        DataTypes.ATTR,
                        SavingStream.INFO);
        public static final Topic PED_IDRANK_SAVING_TOPIC =
                new Topic("pedestrian-idrank-saving",
                        DataTypes.IDRANK,
                        SavingStream.INFO);
        private Map<String, String> kafkaParams = new HashMap<>();
        private String metadataDir;
        // Create KafkaSink for Spark Streaming to output to Kafka.
        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<FileSystem> hdfsSingleton;
        private Singleton<GraphDatabaseConnector> dbConnSingleton;
        private final int procTime;

        public SavingStream(@Nonnull SystemPropertyCenter propCenter) throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            this.procTime = propCenter.procTime;

            // Common Kafka settings
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,
                    INFO.NAME);
//            kafkaParams.put("zookeeper.connect", propCenter.zkConn);
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    propCenter.kafkaBootstrapServers);
            kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "largest");
            kafkaParams.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put("fetch.message.max.bytes",
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());
            kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);
            kafkaParams.put(ConsumerConfig.SEND_BUFFER_CONFIG,
                    "" + propCenter.kafkaMsgMaxBytes);

            metadataDir = propCenter.metadataDir;

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

            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(
                    producerProp));
            hdfsSingleton = new Singleton<>(new HDFSFactory());
            dbConnSingleton = new Singleton<>(() -> new FakeDatabaseConnector());
        }

        /**
         * Store the track to the HDFS.
         *
         * @param storeDir The directory storing the track.
         * @param tracklet The track to store.
         * @throws IOException On failure creating and writing files in HDFS.
         */
        private void storeTracklet(@Nonnull String storeDir,
                                   @Nonnull Tracklet tracklet) throws Exception {
            FileSystem hdfs = hdfsSingleton.getInst();

            // Write verbal informations with Json.
            FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/info.txt"));

            // Customize the serialization of bounding box in order to ignore patch data.
            GsonBuilder gsonBuilder = new GsonBuilder();
            JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
                JsonObject result = new JsonObject();
                result.add("x", new JsonPrimitive(box.x));
                result.add("y", new JsonPrimitive(box.y));
                result.add("width", new JsonPrimitive(box.width));
                result.add("height", new JsonPrimitive(box.height));
                return result;
            };
            gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);
            outputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
            outputStream.close();

            // Write frames.
            for (int i = 0; i < tracklet.locationSequence.length; ++i) {
                Tracklet.BoundingBox bbox = tracklet.locationSequence[i];

                // Use JavaCV to encode the image patch
                // into JPEG, stored in the memory.
                BytePointer inputPointer = new BytePointer(bbox.patchData);
                Mat image = new Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
                BytePointer outputPointer = new BytePointer();
                imencode(".jpg", image, outputPointer);
                byte[] bytes = new byte[(int) outputPointer.limit()];
                outputPointer.get(bytes);

                // Output the image patch to HDFS.
                FSDataOutputStream imgOutputStream = hdfs.create(new Path(storeDir + "/" + i + ".jpg"));
                imgOutputStream.write(bytes);
                imgOutputStream.close();

                image.release();
                inputPointer.deallocate();
                outputPointer.deallocate();
            }
        }

        @Override
        public void addToContext(@Nonnull JavaStreamingContext jssc) {// Save tracklets.
            buildBytesDirectStream(jssc, Arrays.asList(PED_TRACKLET_SAVING_TOPIC.NAME), kafkaParams, procTime)
                    .foreachRDD(rdd -> rdd.foreach(kvPair -> {
                        try {
                            // RuntimeException: No native JavaCPP library
                            // in memory. (Has Loader.load() been called?)
                            Loader.load(opencv_core.class);
                            Loader.load(opencv_imgproc.class);

                            FileSystem hdfs = hdfsSingleton.getInst();

                            String taskID = kvPair._1();
                            TaskData taskData;
                            taskData = (TaskData) deserialize(kvPair._2());
                            Tracklet tracklet = (Tracklet) taskData.predecessorRes;
                            int numTracklets = tracklet.numTracklets;
                            String videoRoot = metadataDir + "/" + tracklet.id.videoID;
                            String taskRoot = videoRoot + "/" + taskID;
                            hdfs.mkdirs(new Path(taskRoot));

                            loggerSingleton.getInst().info("Task " + taskID + " got track: " + tracklet.id + "!");

                            String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                            hdfs.mkdirs(new Path(storeDir));

                            storeTracklet(storeDir, tracklet);

                            // If all the tracklets from a task are saved,
                            // it's time to pack them into a HAR!
                            ContentSummary contentSummary = hdfs.getContentSummary(new Path(taskRoot));
                            long cnt = contentSummary.getDirectoryCount();
                            // Decrease one for directory counter.
                            if (cnt - 1 == numTracklets) {
                                loggerSingleton.getInst()
                                        .info("Starting to pack metadata for Task " + taskID
                                                + "(" + tracklet.id.videoID + ")!");

                                HadoopArchives arch = new HadoopArchives(new Configuration());
                                ArrayList<String> opt = new ArrayList<>();
                                opt.add("-archiveName");
                                opt.add(taskID + ".har");
                                opt.add("-p");
                                opt.add(taskRoot);
                                opt.add(videoRoot);
                                arch.run(Arrays.copyOf(opt.toArray(), opt.size(), String[].class));

                                loggerSingleton.getInst().info(
                                        "Task " + taskID + "(" + tracklet.id.videoID + ") packed!");

                                dbConnSingleton.getInst().setTrackSavingPath(tracklet.id.videoID,
                                        videoRoot + "/" + taskID + ".har");

                                // Delete the original folder recursively.
                                hdfs.delete(new Path(taskRoot), true);
                            } else {
                                loggerSingleton.getInst().info("Task " + taskID
                                        + "(" + tracklet.id.videoID + ") need "
                                        + (numTracklets - cnt + 1) + "/" + numTracklets + " more tracklets!");
                            }
                        } catch (Exception e) {
                            loggerSingleton.getInst().error("During storing tracklets.", e);
                        }
                    }));

            // Display the attributes.
            // TODO Modify the streaming steps from here to store the meta data.
            buildBytesDirectStream(jssc, Arrays.asList(PED_ATTR_SAVING_TOPIC.NAME), kafkaParams, procTime)
                    .foreachRDD(rdd -> rdd.foreach(res -> {
                        try {
                            TaskData taskData;
                            taskData = (TaskData) deserialize(res._2());
                            Attributes attr = (Attributes) taskData.predecessorRes;

                            loggerSingleton.getInst().debug("Received " + res._1() + ": " + attr);

                            dbConnSingleton.getInst().setPedestrianAttributes(attr.trackletID.toString(), attr);

                            loggerSingleton.getInst().debug("Saved " + res._1() + ": " + attr);
                        } catch (Exception e) {
                            loggerSingleton.getInst().error("When decompressing attributes", e);
                        }
                    }));

            // Display the id ranks.
            // TODO Modify the streaming steps from here to store the meta data.
            buildBytesDirectStream(jssc, Arrays.asList(PED_IDRANK_SAVING_TOPIC.NAME), kafkaParams, procTime)
                    .foreachRDD(rdd -> rdd.foreach(res -> {
                        int[] idRank;
                        try {
                            TaskData taskData;
                            taskData = (TaskData) deserialize(res._2());
                            idRank = (int[]) taskData.predecessorRes;
                            String rankStr = "";
                            for (int id : idRank) {
                                rankStr = rankStr + id + " ";
                            }
                            loggerSingleton.getInst().info("Metadata saver received: " + res._1()
                                    + ": Pedestrian IDRANK rank: " + rankStr);
                            //TODO(Ken Yu): Save IDs to database.
                        } catch (Exception e) {
                            loggerSingleton.getInst()
                                    .error("When decompressing IDRANK", e);
                        }
                    }));
        }
    }
}
