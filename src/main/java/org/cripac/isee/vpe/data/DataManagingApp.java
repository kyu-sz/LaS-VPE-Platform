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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.helper.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.common.Topic;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.cripac.isee.vpe.util.FFmpegFrameGrabberNew;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.retrieveTracklet;

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
    private static final long serialVersionUID = 7338424132131492017L;

    public DataManagingApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Arrays.asList(
                new PedestrainTrackletRetrievingStream(propCenter),
                new PedestrainTrackletAttrRetrievingStream(propCenter),
                new TrackletSavingStream(propCenter),
                new AttrSavingStream(propCenter),
                new IDRankSavingStream(propCenter),
                new VideoCuttingStream(propCenter)));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {

        private static final long serialVersionUID = -786439769732467646L;

        int maxFramePerFragment = 1000;

        public AppPropertyCenter(@Nonnull String[] args)
                throws URISyntaxException, ParserConfigurationException, SAXException, UnknownHostException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.max.frame.per.fragment":
                        maxFramePerFragment = new Integer((String) entry.getValue());
                        break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final AppPropertyCenter propCenter = new AppPropertyCenter(args);

        Thread packingThread = new Thread(new TrackletPackingThread(propCenter));
        packingThread.start();

        final SparkStreamingApp app = new DataManagingApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class VideoCuttingStream extends Stream {

        public final static Topic VIDEO_URL_TOPIC = new Topic("video-url-for-cutting", DataType.URL);
        private static final long serialVersionUID = -6187153660239066646L;
        public static final DataType OUTPUT_TYPE = DataType.FRAME_ARRAY;
        private final Singleton<FileSystem> hdfsSingleton;

        int maxFramePerFragment;

        /**
         * Initialize necessary components of a Stream object.
         *
         * @param propCenter System property center.
         * @throws Exception On failure creating singleton.
         */
        public VideoCuttingStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
            hdfsSingleton = new Singleton<>(new HDFSFactory());
            maxFramePerFragment = propCenter.maxFramePerFragment;
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {
            this.<TaskData<String>>filter(globalStream, VIDEO_URL_TOPIC)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            final String taskID = kv._1();
                            final TaskData<String> taskData = kv._2();

                            FFmpegFrameGrabberNew frameGrabber = new FFmpegFrameGrabberNew(
                                    hdfsSingleton.getInst().open(new Path(taskData.predecessorRes))
                            );

                            Frame[] fragments = new Frame[maxFramePerFragment];
                            int cnt = 0;
                            while (true) {
                                Frame frame;
                                try {
                                    frame = frameGrabber.grabImage();
                                } catch (FrameGrabber.Exception e) {
                                    logger.error("On grabImage: " + e);
                                    if (cnt > 0) {
                                        Frame[] lastFragments = new Frame[cnt];
                                        System.arraycopy(fragments, 0, lastFragments, 0, cnt);
                                        output(taskData.curNode.getOutputPorts(),
                                                taskData.executionPlan, lastFragments, taskID);
                                    }
                                    break;
                                }
                                if (frame == null) {
                                    if (cnt > 0) {
                                        Frame[] lastFragments = new Frame[cnt];
                                        System.arraycopy(fragments, 0, lastFragments, 0, cnt);
                                        output(taskData.curNode.getOutputPorts(),
                                                taskData.executionPlan, lastFragments, taskID);
                                    }
                                    break;
                                }

                                fragments[cnt++] = frame;
                                if (cnt >= maxFramePerFragment) {
                                    output(taskData.curNode.getOutputPorts(),
                                            taskData.executionPlan, fragments, taskID);
                                    cnt = 0;
                                }
                            }
                        } catch (Throwable t) {
                            logger.error("On cutting video", t);
                        }
                    }));
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(VIDEO_URL_TOPIC.NAME);
        }
    }

    public static class PedestrainTrackletRetrievingStream extends Stream {

        public static final String NAME = "pedestrian-tracklet-rtrv";
        public static final DataType OUTPUT_TYPE = DataType.TRACKLET;
        public static final Topic RTRV_JOB_TOPIC =
                new Topic("pedestrian-tracklet-rtrv-job", DataType.TRACKLET_ID);
        private static final long serialVersionUID = -3588633503578388408L;
        // Create KafkaSink for Spark Streaming to output to Kafka.
        private final Singleton<GraphDatabaseConnector> dbConnSingleton;

        public PedestrainTrackletRetrievingStream(SystemPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            dbConnSingleton = new Singleton<>(FakeDatabaseConnector::new);
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {
            // Read track retrieving jobs in parallel from Kafka.
            // URL of a video is given.
            // The directory storing the tracklets of the video is stored in the database.
            this.<TaskData<Tracklet.Identifier>>filter(globalStream, RTRV_JOB_TOPIC)
                    // Retrieve and deliver tracklets.
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                                final Logger logger = loggerSingleton.getInst();

                                try {
                                    // Recover task data.
                                    final TaskData<Tracklet.Identifier> taskData = kv._2();
                                    final Tracklet.Identifier trackletID = taskData.predecessorRes;

                                    // Retrieve the track from HDFS.
                                    // Store the track to a task data (reused).
                                    final Tracklet tracklet = retrieveTracklet(
                                            dbConnSingleton.getInst().getTrackletSavingDir(trackletID.videoID),
                                            trackletID,
                                            loggerSingleton.getInst());

                                    // Get the IDs of successor nodes.
                                    final List<TaskData.ExecutionPlan.Node.Port> outputPorts =
                                            taskData.curNode.getOutputPorts();
                                    // Mark the current node as executed.
                                    taskData.curNode.markExecuted();

                                    // Send to all the successor nodes.
                                    final String taskID = kv._1();
                                    output(outputPorts, taskData.executionPlan, tracklet, taskID);
                                } catch (Exception e) {
                                    logger.error("During retrieving tracklets", e);
                                }
                            })
                    );
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(RTRV_JOB_TOPIC.NAME);
        }
    }

    public static class PedestrainTrackletAttrRetrievingStream extends Stream {
        public static final String NAME = "pedestrian-tracklet-attr-rtrv";
        public static final DataType OUTPUT_TYPE = DataType.TRACKLET_ATTR;
        public static final Topic RTRV_JOB_TOPIC =
                new Topic("pedestrian-tracklet-attr-rtrv-job", DataType.TRACKLET_ID);
        private static final long serialVersionUID = -8876416114616771091L;
        // Create KafkaSink for Spark Streaming to output to Kafka.
        private final Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private final Singleton<GraphDatabaseConnector> dbConnSingleton;

        public PedestrainTrackletAttrRetrievingStream(SystemPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            final Properties producerProp = propCenter.getKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));
            dbConnSingleton = new Singleton<>(FakeDatabaseConnector::new);
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {
            // Read track with attributes retrieving jobs in parallel from Kafka.
            this.<TaskData<Tracklet.Identifier>>filter(globalStream, RTRV_JOB_TOPIC)
                    // Retrieve and deliver tracklets with attributes.
                    .foreachRDD(rdd -> rdd.foreach(job -> {
                                final Logger logger = loggerSingleton.getInst();
                                try {
                                    final String taskID = job._1();
                                    // Recover task data.
                                    final TaskData<Tracklet.Identifier> taskData = job._2();
                                    // Get parameters for the job.
                                    final Tracklet.Identifier trackletID = taskData.predecessorRes;
                                    final String videoURL = trackletID.videoID;

                                    final PedestrianInfo info = new PedestrianInfo();
                                    // Retrieve the track from HDFS.
                                    info.tracklet = retrieveTracklet(
                                            dbConnSingleton.getInst().getTrackletSavingDir(videoURL),
                                            trackletID,
                                            logger);
                                    // Retrieve the attributes from database.
                                    info.attr = dbConnSingleton.getInst().getPedestrianAttributes(trackletID.toString());

                                    // Get the IDs of successor nodes.
                                    final List<TaskData.ExecutionPlan.Node.Port> outputPorts
                                            = taskData.curNode.getOutputPorts();
                                    // Mark the current node as executed.
                                    taskData.curNode.markExecuted();

                                    // Send to all the successor nodes.
                                    output(outputPorts, taskData.executionPlan, info, taskID);
                                } catch (Exception e) {
                                    logger.error("During retrieving tracklet and attributes", e);
                                }
                            })
                    );
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(RTRV_JOB_TOPIC.NAME);
        }
    }

    /**
     * This is a thread independent from Spark Streaming,
     * which listen to tracklet packing jobs from Kafka,
     * and perform HAR packing. There is no need to worry
     * about job loss due to system faults, since offsets
     * are committed after jobs are finished, so interrupted
     * jobs can be retrieved from Kafka and executed again
     * on another start of this thread. This thread is to be
     * started together with the DataManagingApp.
     */
    static class TrackletPackingThread implements Runnable {

        final static Topic JOB_TOPIC = new Topic("tracklet-packing-job", DataType.URL);

        final Properties consumerProperties;
        final String metadataDir;
        final Logger logger;
        final GraphDatabaseConnector databaseConnector;

        TrackletPackingThread(SystemPropertyCenter propCenter) {
            consumerProperties = propCenter.getKafkaConsumerProp("tracklet-packing", true);
            metadataDir = propCenter.metadataDir;
            logger = new SynthesizedLogger(APP_NAME, propCenter);
            databaseConnector = new FakeDatabaseConnector();
        }

        @Override
        public void run() {
            KafkaConsumer<String, String> jobListener = new KafkaConsumer<>(consumerProperties);
            jobListener.subscribe(Collections.singletonList(JOB_TOPIC.NAME));
            while (true) {
                ConsumerRecords<String, String> records = jobListener.poll(1000);
                logger.debug("Tracklet packing thread received " + records.count() + " jobs!");
                records.forEach(rec -> {
                    final String taskID = rec.key();
                    final String videoID = rec.value();
                    final String videoRoot = metadataDir + "/" + videoID;
                    final String taskRoot = videoRoot + "/" + taskID;

                    logger.info("Starting to pack metadata for Task " + taskID + "(" + videoID + ")!");

                    final HadoopArchives arch = new HadoopArchives(new Configuration());
                    final ArrayList<String> harPackingOptions = new ArrayList<>();
                    harPackingOptions.add("-archiveName");
                    harPackingOptions.add(taskID + ".har");
                    harPackingOptions.add("-p");
                    harPackingOptions.add(taskRoot);
                    harPackingOptions.add(videoRoot);
                    try {
                        arch.run(Arrays.copyOf(harPackingOptions.toArray(),
                                harPackingOptions.size(), String[].class));
                    } catch (Exception e) {
                        logger.error("On running archiving", e);
                    }

                    logger.info("Task " + taskID + "(" + videoID + ") packed!");

                    databaseConnector.setTrackSavingPath(videoID, videoRoot + "/" + taskID + ".har");

                    // Delete the original folder recursively.
                    try {
                        new HDFSFactory().produce().delete(new Path(taskRoot), true);
                    } catch (IOException e) {
                        logger.error("On deleting original task folder", e);
                    }
                });
                jobListener.commitSync();
            }
        }
    }

    public static class TrackletSavingStream extends Stream {
        public static final String NAME = "tracklet-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Topic PED_TRACKLET_SAVING_TOPIC =
                new Topic("pedestrian-tracklet-saving", DataType.TRACKLET);
        private static final long serialVersionUID = 2820895755662980265L;
        private final Singleton<FileSystem> hdfsSingleton;
        private final String metadataDir;
        private final Singleton<KafkaProducer<String, String>> packingJobProducerSingleton;

        TrackletSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            hdfsSingleton = new Singleton<>(new HDFSFactory());
            metadataDir = propCenter.metadataDir;
            packingJobProducerSingleton = new Singleton<>(
                    new KafkaProducerFactory<>(propCenter.getKafkaProducerProp(true))
            );
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
            final FileSystem hdfs = hdfsSingleton.getInst();

            // Write verbal informations with Json.
            final FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/info.txt"));

            // Customize the serialization of bounding box in order to ignore patch data.
            final GsonBuilder gsonBuilder = new GsonBuilder();
            final JsonSerializer<Tracklet.BoundingBox> bboxSerializer = (box, typeOfBox, context) -> {
                JsonObject result = new JsonObject();
                result.add("x", new JsonPrimitive(box.x));
                result.add("y", new JsonPrimitive(box.y));
                result.add("width", new JsonPrimitive(box.width));
                result.add("height", new JsonPrimitive(box.height));
                return result;
            };
            gsonBuilder.registerTypeAdapter(Tracklet.BoundingBox.class, bboxSerializer);

            // Write serialized basic information of the tracklet to HDFS.
            outputStream.writeBytes(gsonBuilder.create().toJson(tracklet));
            outputStream.close();

            // Write frames concurrently.
            ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
                    .parallelStream()
                    .forEach(idx -> {
                        final Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];

                        // Use JavaCV to encode the image patch
                        // into JPEG, stored in the memory.
                        final BytePointer inputPointer = new BytePointer(bbox.patchData);
                        final Mat image = new Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
                        final BytePointer outputPointer = new BytePointer();
                        imencode(".jpg", image, outputPointer);
                        final byte[] bytes = new byte[(int) outputPointer.limit()];
                        outputPointer.get(bytes);

                        // Output the image patch to HDFS.
                        final FSDataOutputStream imgOutputStream;
                        try {
                            imgOutputStream = hdfs.create(new Path(storeDir + "/" + idx + ".jpg"));
                            imgOutputStream.write(bytes);
                            imgOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        // Free resources.
                        image.release();
                        inputPointer.deallocate();
                        outputPointer.deallocate();
                    });
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {// Save tracklets.
            this.<TaskData<Tracklet>>filter(globalStream, PED_TRACKLET_SAVING_TOPIC)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            // RuntimeException: No native JavaCPP library
                            // in memory. (Has Loader.load() been called?)
                            Loader.load(opencv_core.class);
                            Loader.load(opencv_imgproc.class);

                            final FileSystem hdfs = hdfsSingleton.getInst();

                            final String taskID = kv._1();
                            final TaskData taskData = kv._2();
                            final Tracklet tracklet = (Tracklet) taskData.predecessorRes;
                            final int numTracklets = tracklet.numTracklets;

                            final String videoRoot = metadataDir + "/" + tracklet.id.videoID;
                            final String taskRoot = videoRoot + "/" + taskID;
                            final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                            hdfs.mkdirs(new Path(storeDir));

                            storeTracklet(storeDir, tracklet);

                            // If all the tracklets from a task are saved,
                            // it's time to pack them into a HAR!
                            final ContentSummary contentSummary = hdfs.getContentSummary(new Path(taskRoot));
                            final long dirCnt = contentSummary.getDirectoryCount();
                            // Decrease one for directory counter.
                            if (dirCnt - 1 == numTracklets) {
                                logger.info("Should pack metadata for Task " + taskID
                                        + "(" + tracklet.id.videoID + ")! The directory consumes "
                                        + contentSummary.getSpaceConsumed() + " bytes.");

                                KafkaHelper.sendWithLog(TrackletPackingThread.JOB_TOPIC, taskID, tracklet.id.videoID,
                                        packingJobProducerSingleton.getInst(), logger);
                            } else {
                                logger.info("Task " + taskID + "(" + tracklet.id.videoID + ") need "
                                        + (numTracklets - dirCnt + 1) + "/" + numTracklets + " more tracklets!");
                            }
                        } catch (Exception e) {
                            logger.error("During storing tracklets.", e);
                        }
                    }));
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(PED_TRACKLET_SAVING_TOPIC.NAME);
        }
    }

    public static class AttrSavingStream extends Stream {
        public static final String NAME = "attr-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Topic PED_ATTR_SAVING_TOPIC =
                new Topic("pedestrian-attr-saving", DataType.ATTR);
        private static final long serialVersionUID = 858443725387544606L;
        private final Singleton<GraphDatabaseConnector> dbConnSingleton;

        AttrSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            dbConnSingleton = new Singleton<>(FakeDatabaseConnector::new);
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {
            // Display the attributes.
            // TODO Modify the streaming steps from here to store the meta data.
            this.<TaskData<Tracklet>>filter(globalStream, PED_ATTR_SAVING_TOPIC)
                    .foreachRDD(rdd -> rdd.foreach(res -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            final TaskData taskData = res._2();
                            final Attributes attr = (Attributes) taskData.predecessorRes;

                            logger.debug("Received " + res._1() + ": " + attr);

                            dbConnSingleton.getInst().setPedestrianAttributes(attr.trackletID.toString(), attr);

                            logger.debug("Saved " + res._1() + ": " + attr);
                        } catch (Exception e) {
                            logger.error("When decompressing attributes", e);
                        }
                    }));
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(PED_ATTR_SAVING_TOPIC.NAME);
        }
    }

    public static class IDRankSavingStream extends Stream {
        public static final String NAME = "idrank-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Topic PED_IDRANK_SAVING_TOPIC =
                new Topic("pedestrian-idrank-saving", DataType.IDRANK);
        private static final long serialVersionUID = -6469177153696762040L;

        public IDRankSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {
            // Display the id ranks.
            // TODO Modify the streaming steps from here to store the meta data.
            this.<TaskData<Tracklet>>filter(globalStream, PED_IDRANK_SAVING_TOPIC)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            final TaskData taskData = kv._2();
                            final int[] idRank = (int[]) taskData.predecessorRes;
                            String rankStr = "";
                            for (int id : idRank) {
                                rankStr = rankStr + id + " ";
                            }
                            logger.info("Metadata saver received: " + kv._1()
                                    + ": Pedestrian IDRANK rank: " + rankStr);
                            //TODO(Ken Yu): Save IDs to database.
                        } catch (Exception e) {
                            logger.error("When decompressing IDRANK", e);
                        }
                    }));
        }

        @Override
        public List<String> listeningTopics() {
            return Collections.singletonList(PED_IDRANK_SAVING_TOPIC.NAME);
        }
    }
}
