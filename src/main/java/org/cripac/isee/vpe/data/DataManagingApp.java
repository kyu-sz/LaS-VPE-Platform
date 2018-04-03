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

package org.cripac.isee.vpe.data;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.debug.FakeDatabaseConnector;
import org.cripac.isee.util.SerializationHelper;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.kafka.ByteArrayProducer;
import org.cripac.isee.vpe.util.kafka.ByteArrayProducerFactory;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.xml.sax.SAXException;
import scala.Tuple2;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.cripac.isee.util.SerializationHelper.serialize;

/**
 * The DataManagingApp class combines two functions: meta data saving and data
 * feeding. The meta data saving function saves meta data, which may be the
 * results of vision algorithms, to HDFS and Neo4j database. The data feeding
 * function retrieves stored results and sendWithLog them to algorithm modules from
 * HDFS and Neo4j database. The reason why combine these two functions is that
 * they should both be modified when and only when a new data inputType shall be
 * supported by the system, and they require less resources than other modules,
 * so combining them can save resources while not harming performance.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class DataManagingApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "data-managing";
    private static final long serialVersionUID = 7338424132131492017L;

    public DataManagingApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Arrays.asList(
                new TrackletSavingStream(propCenter),
                new AttrSavingStream(propCenter),
                new IDRankSavingStream(propCenter)
//                ,new VideoCuttingStream(propCenter)
                ));
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
                        maxFramePerFragment = Integer.parseInt((String) entry.getValue());
                        break;
                    default:
                        logger.warn("Unrecognized option: " + entry.getKey());
                        break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final AppPropertyCenter propCenter = new AppPropertyCenter(args);

//        AtomicReference<Boolean> running = new AtomicReference<>();
//        running.set(true);

//        Thread packingThread = new Thread(new TrackletPackingThread(propCenter, running));
//        packingThread.start();

        final SparkStreamingApp app = new DataManagingApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
//        running.set(false);
    }

    public static class VideoCuttingStream extends Stream {

        public final static Port VIDEO_URL_PORT = new Port("video-url-for-cutting", DataType.URL);
        private static final long serialVersionUID = -6187153660239066646L;
        public static final DataType OUTPUT_TYPE = DataType.FRAME_ARRAY;

        int maxFramePerFragment;

        /**
         * Initialize necessary components of a Stream object.
         *
         * @param propCenter System property center.
         * @throws Exception On failure creating singleton.
         */
        public VideoCuttingStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
            maxFramePerFragment = propCenter.maxFramePerFragment;
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
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (VideoCuttingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            ParallelExecutor.execute(kvIter, kv -> {
                                try {
                                    new RobustExecutor<Void, Void>(() -> {
                                        final UUID taskID = kv._1();
                                        final TaskData taskData = kv._2();

                                        final FileSystem hdfs = HDFSFactory.newInstance();
                                        FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(
                                                hdfs.open(new Path((String) taskData.predecessorRes))
                                        );

                                        Frame[] fragments = new Frame[maxFramePerFragment];
                                        int cnt = 0;
                                        final ExecutionPlan.Node curNode = taskData.getDestNode(VIDEO_URL_PORT);
                                        assert curNode != null;
                                        final List<ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                                        curNode.markExecuted();
                                        while (true) {
                                            Frame frame;
                                            try {
                                                frame = frameGrabber.grabImage();
                                            } catch (FrameGrabber.Exception e) {
                                                logger.error("On grabImage: " + e);
                                                if (cnt > 0) {
                                                    Frame[] lastFragments = new Frame[cnt];
                                                    System.arraycopy(fragments, 0, lastFragments, 0, cnt);
                                                    output(outputPorts, taskData.executionPlan, lastFragments, taskID);
                                                }
                                                break;
                                            }
                                            if (frame == null) {
                                                if (cnt > 0) {
                                                    Frame[] lastFragments = new Frame[cnt];
                                                    System.arraycopy(fragments, 0, lastFragments, 0, cnt);
                                                    output(outputPorts, taskData.executionPlan, lastFragments, taskID);
                                                }
                                                break;
                                            }

                                            fragments[cnt++] = frame;
                                            if (cnt >= maxFramePerFragment) {
                                                output(outputPorts, taskData.executionPlan, fragments, taskID);
                                                cnt = 0;
                                            }
                                        }
                                    }).execute();
                                } catch (Throwable t) {
                                    logger.error("On cutting video", t);
                                }
                            });
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(VIDEO_URL_PORT);
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

        final static String JOB_TOPIC = "tracklet-packing-job";

        final Properties consumerProperties;
        final String metadataDir;
        final Logger logger;
        private final AtomicReference<Boolean> running;
        final GraphDatabaseConnector dbConnector;
        private final static int MAX_POLL_INTERVAL_MS = 300000;
        private int maxPollRecords = 500;

        TrackletPackingThread(AppPropertyCenter propCenter, AtomicReference<Boolean> running) {
            consumerProperties = propCenter.getKafkaConsumerProp("tracklet-packing", false);
            consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "" + maxPollRecords);
            consumerProperties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "" + MAX_POLL_INTERVAL_MS);
            metadataDir = propCenter.metadataDir;
            logger = new SynthesizedLogger(APP_NAME, propCenter);
            this.running = running;
            // dbConnector = new FakeDatabaseConnector();
            dbConnector = new Neo4jConnector();
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    KafkaConsumer<String, byte[]> jobListener = new KafkaConsumer<>(consumerProperties);
                    final FileSystem hdfs;
                    FileSystem tmpHDFS;
                    while (true) {
                        try {
                            tmpHDFS = new HDFSFactory().produce();
                            break;
                        } catch (IOException e) {
                            logger.error("On connecting HDFS", e);
                        }
                    }
                    hdfs = tmpHDFS;
                    jobListener.subscribe(Collections.singletonList(JOB_TOPIC));
                    while (running.get()) {
                        ConsumerRecords<String, byte[]> records = jobListener.poll(1000);
                        Map<String, byte[]> taskMap = new HashMap<>();
                        records.forEach(rec -> taskMap.put(rec.key(), rec.value()));

                        final long start = System.currentTimeMillis();
                        logger.info("开始时间："+start);
                        logger.info("Packing thread received " + taskMap.keySet().size() + " jobs.");
                        //TODO(Ken Yu): Make sure whether executing HAR packing in parallel is faster.
                        //TODO(Ken Yu): Find the best parallelism.
                        ParallelExecutor.execute(taskMap.entrySet(), 4, kv -> {
                            try {
                                final String taskID = kv.getKey();
                                final byte[] value = kv.getValue();
                                //final Tuple2<String, Integer> info = SerializationHelper.deserialize(value);
                                //final String videoID = info._1();
                                // Modified "String" in Tuple2 to "Tracklet.Identifier" on 2017/04/26
                                final Tuple2<Tracklet.Identifier, Integer> info = SerializationHelper.deserialize(value);
                                final Tracklet.Identifier trackletID = info._1();
                                final String videoID = trackletID.videoID;
                                final int numTracklets = info._2();
                                final String videoRoot = metadataDir + "/" + videoID;
                                final String taskRoot = videoRoot + "/" + taskID;

                                /*final boolean harExists = new RobustExecutor<Void, Boolean>(
                                        (Function0<Boolean>) () ->
                                                hdfs.exists(new Path(videoRoot + "/" + taskID + ".har"))
                                ).execute();
                                if (harExists) {
                                    // Packing has been finished in a previous request.
                                    final boolean taskRootExists = new RobustExecutor<Void, Boolean>(
                                            (Function0<Boolean>) () ->
                                                    hdfs.exists(new Path(videoRoot + "/" + taskID))
                                    ).execute();
                                    if (taskRootExists) {
                                        // But seems to have failed to delete the task root.
                                        // Now do it again.
                                        new RobustExecutor<Void, Void>(() ->
                                                hdfs.delete(new Path(taskRoot), true)
                                        ).execute();
                                    }
                                    return;
                                }*/

                                // If all the tracklets from a task are saved,
                                // it's time to pack them into a HAR!
                                final ContentSummary contentSummary = new RobustExecutor<Void, ContentSummary>(
                                        (Function0<ContentSummary>) () -> hdfs.getContentSummary(new Path(taskRoot))
                                ).execute();
                                final long dirCnt = contentSummary.getDirectoryCount();
                                // Decrease one for directory counter.
                                if (dirCnt - 1 == numTracklets) {
                                   /* logger.info("Starting to pack tracklets for task " + taskID
                                            + "(" + videoID + ")! The directory consumes "
                                            + contentSummary.getSpaceConsumed() + " bytes.");

                                    new RobustExecutor<Void, Void>(() -> {
                                        final HadoopArchives arch = new HadoopArchives(HadoopHelper.getDefaultConf());
                                        final ArrayList<String> harPackingOptions = new ArrayList<>();
                                        harPackingOptions.add("-archiveName");
                                        harPackingOptions.add(taskID + ".har");
                                        harPackingOptions.add("-p");
                                        harPackingOptions.add(taskRoot);
                                        harPackingOptions.add(videoRoot);
                                        int ret = arch.run(Arrays.copyOf(harPackingOptions.toArray(),
                                                harPackingOptions.size(), String[].class));
                                        if (ret < 0) {
                                            throw new IOException("Packing tracklets for task "
                                                    + taskID + "(" + videoID + ") failed.");
                                        }
                                    }).execute();

                                    logger.info("Task " + taskID + "(" + videoID + ") packed!");
                                    */
                                    // Set the HAR path to all the tracklets from this video.
                                    for (int i = 0; i < numTracklets; ++i) {
                                    	logger.info("保存到neo4j中---------------"+i);
                                        new RobustExecutor<Integer, Void>((VoidFunction<Integer>) idx ->{
                                                dbConnector.setTrackletSavingPath(
                                                        new Tracklet.Identifier(videoID, idx).toString(),
                                                        videoRoot + "/" + taskID + ".har/" + idx,logger);
                                                dbConnector.setTrackletSavingPathFlag(
                                                		new Tracklet.Identifier(videoID, idx).toString(),
                                                		true,logger);
                                              /*  dbConnector.setTrackletSavingVideoPath(
                                                		new Tracklet.Identifier(tracklet.id.videoID, idx).toString(), 
                                                		storeDir);
                                                dbConnector.setSaveTracklet(tracklet);*/
                                                
                                        }).execute(i);
                                        
                                        
                                    }

                                    // Delete the original folder recursively.
                                    new RobustExecutor<Void, Void>(() ->
                                            new HDFSFactory().produce().delete(new Path(taskRoot), true)
                                    ).execute();
                                } else {
                                    logger.info("Task " + taskID + "(" + videoID + ") need "
                                            + (numTracklets - dirCnt + 1) + "/" + numTracklets + " more tracklets!");
                                }
                            } catch (Exception e) {
                                logger.error("On trying to pack tracklets", e);
                            }
                        });
                        final long end = System.currentTimeMillis();
                        logger.info("结束时间："+end);
                        try {
                            jobListener.commitSync();
                            if (records.count() >= maxPollRecords && end - start < MAX_POLL_INTERVAL_MS) {
                                // Can poll more records once, and there are many records in Kafka waiting to be processed.
                                maxPollRecords = maxPollRecords * 3 / 2;
                                consumerProperties.setProperty("max.poll.records", "" + maxPollRecords);
                                jobListener = new KafkaConsumer<>(consumerProperties);
                                jobListener.subscribe(Collections.singletonList(JOB_TOPIC));
                            }
                        } catch (CommitFailedException e) {
                            // Processing time is longer than poll interval.
                            // Poll fewer records once.
                            maxPollRecords /= 2;
                            consumerProperties.setProperty("max.poll.records", "" + maxPollRecords);
                            jobListener = new KafkaConsumer<>(consumerProperties);
                            jobListener.subscribe(Collections.singletonList(JOB_TOPIC));
                        }
                    }
                    hdfs.close();
                } catch (Exception e) {
                    logger.error("In packing thread", e);
                }
            }
        }
    }

    public static class TrackletSavingStream extends Stream {
        public static final String NAME = "tracklet-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_TRACKLET_SAVING_PORT =
                new Port("pedestrian-tracklet-saving", DataType.TRACKLET);
        private static final long serialVersionUID = 2820895755662980265L;
        private final String metadataDir;
        private final Singleton<ByteArrayProducer> packingJobProducerSingleton;
//        final GraphDatabaseConnector dbConnector;
//        final Logger logger;
        
        TrackletSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            metadataDir = propCenter.metadataDir;
            packingJobProducerSingleton = new Singleton<>(
                    new ByteArrayProducerFactory(propCenter.getKafkaProducerProp(false)),
                    ByteArrayProducer.class);
//            dbConnector = new Neo4jConnector();
//            logger=new ConsoleLogger();
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
            // Save tracklets.
            this.filter(globalStreamMap, PED_TRACKLET_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (TrackletSavingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            final GraphDatabaseConnector dbConnector=dbConnSingleton.getInst();
                            ParallelExecutor.execute(kvIter, kv -> {
                                try {
                                    final FileSystem hdfs = HDFSFactory.newInstance();
                                    final UUID taskID = kv._1();
                                    final TaskData taskData = kv._2();
                                    final TrackletOrURL trackletOrURL = (TrackletOrURL) taskData.predecessorRes;
                                    final Tracklet tracklet = trackletOrURL.getTracklet(logger);
                                    final int numTracklets = tracklet.numTracklets;

                                    if (trackletOrURL.isStored()) {
                                        // The tracklet has already been stored at HDFS.
                                        logger.debug("Tracklet has already been stored at " + trackletOrURL.getURL()
                                                + ". Skip storing.");
                                    } else {
                                        final String videoRoot = metadataDir + "/new2/" + tracklet.id.videoID;
                                        final String taskRoot = videoRoot + "/" + taskID;
                                        final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                                        logger.info("保存的路径是："+storeDir);
                                        final Path storePath = new Path(storeDir);
                                        trackletOrURL.setURL(storeDir);
                                        new RobustExecutor<Integer, Void>((VoidFunction<Integer>) idx ->{
                                            if (hdfs.exists(storePath)
//                                                    || hdfs.exists(new Path(videoRoot + "/" + taskID + ".har"))
                                                    ) {
                                                logger.warn("Duplicated storing request for " + tracklet.id);
                                            } else {
                                                hdfs.mkdirs(new Path(storeDir));
                                                logger.info("dataMana开始保存图片");
                                                HadoopHelper.storeTracklet(tracklet.id.videoID,storeDir, tracklet, hdfs
//                                                		,dbConnector
                                                		,logger
                                                		);
                                                logger.info("dataMana保存图片结束");
                                                logger.info("开始保存到数据库");
//                                                logger.info("tracklet:"+tracklet.toString());
//                                                String s = "/user/labadmin/metadata/20131223102739-20131223103331/280f5b30-1149-4dfa-9462-12fd8f307edc.har/9";
                                              logger.info("要保存的信息是:"+storeDir);
                                                dbConnector.setTrackletSavingPath(storeDir, storeDir);
//                                                dbConnector.setSaveTracklet(tracklet,logger);
//                                                dbConnector.setTrackletSavingVideoPath(
//                                                		new Tracklet.Identifier(tracklet.id.videoID, idx).toString(), 
//                                                		storeDir);
                                                logger.info("保存到数据库结束");
                                            }
                                        }).execute();
                                    }

                                    // Check packing.
//                                    new RobustExecutor<Void, Void>(() ->
//                                            KafkaHelper.sendWithLog(TrackletPackingThread.JOB_TOPIC,
//                                                    taskID.toString(),
//                                                    serialize(new Tuple2<>(tracklet.id, numTracklets)),
//                                                    packingJobProducerSingleton.getInst(),
//                                                    logger)
//                                    ).execute();
                                    hdfs.close();
                                } catch (Exception e) {
                                    logger.error("During storing tracklets.", e);
                                }
                            });
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_TRACKLET_SAVING_PORT);
        }
    }

    public static class AttrSavingStream extends Stream {
        public static final String NAME = "attr-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_ATTR_SAVING_PORT =
                new Port("pedestrian-attr-saving", DataType.ATTRIBUTES);
        private static final long serialVersionUID = 858443725387544606L;
//        private final Singleton<GraphDatabaseConnector> dbConnSingleton;

        AttrSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

//            dbConnSingleton = new Singleton<>(FakeDatabaseConnector::new, FakeDatabaseConnector.class);
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
            // Display the attributes.
            // TODO Modify the streaming steps from here to store the meta data.
            this.filter(globalStreamMap, PED_ATTR_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (AttrSavingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            final GraphDatabaseConnector dbConnector=dbConnSingleton.getInst();
                            ParallelExecutor.execute(kvIter, res -> {
                                try {
                                    final TaskData taskData = res._2();
                                    final Attributes attr = (Attributes) taskData.predecessorRes;

                                    logger.debug("Received attr:" + res._1() + ": " + attr.toString());

                                    new RobustExecutor<Void, Void>(() ->
////                                            dbConnSingleton.getInst()
                                            dbConnector.setPedestrianAttributes(attr.trackletID.toString(), attr,logger)
                                    ).execute();

                                    logger.debug("Saved attr:" + res._1() + ": " + attr.toString());
                                } catch (Exception e) {
                                    logger.error("When decompressing attributes", e);
                                }
                            });
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_ATTR_SAVING_PORT);
        }
    }

    public static class IDRankSavingStream extends Stream {
        public static final String NAME = "idrank-saving";
        public static final DataType OUTPUT_TYPE = DataType.NONE;
        public static final Port PED_IDRANK_SAVING_PORT =
                new Port("pedestrian-idrank-saving", DataType.IDRANK);
        private static final long serialVersionUID = -6469177153696762040L;

        public IDRankSavingStream(@Nonnull AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);
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
            // Display the id ranks.
            // TODO Modify the streaming steps from here to store the meta data.
            this.filter(globalStreamMap, PED_IDRANK_SAVING_PORT)
                    .foreachRDD(rdd -> rdd.foreachPartition(kvIter -> {
                        synchronized (IDRankSavingStream.class) {
                            final Logger logger = loggerSingleton.getInst();
                            ParallelExecutor.execute(kvIter, kv -> {
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
                            });
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(PED_IDRANK_SAVING_PORT);
        }
    }

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }
}