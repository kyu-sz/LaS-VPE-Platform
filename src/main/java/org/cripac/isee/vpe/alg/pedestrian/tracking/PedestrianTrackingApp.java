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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import kafka.common.FailedToSendMessageException;
import kafka.common.MessageSizeTooLargeException;
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
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TaskData.ExecutionPlan;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

        private final Singleton<Map<String, byte[]>> confBuffer;
        private final int numSamplesPerTracklet;
        private final String metadataDir;

        public HDFSVideoTrackingStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            numSamplesPerTracklet = propCenter.numSamplesPerTracklet;
            metadataDir = propCenter.metadataDir;
            confBuffer = new Singleton<>(Object2ObjectOpenHashMap::new);
        }

        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<String, TaskData>> globalStreamMap) {
            this.filter(globalStreamMap, VIDEO_URL_PORT)
                    .foreachRDD(rdd -> rdd.glom().foreach(kvList -> {
                        final Logger logger = loggerSingleton.getInst();
                        if (kvList.size() > 0) {
                            logger.info("Partition " + TaskContext.getPartitionId()
                                    + " got " + kvList.size()
                                    + " videos in this batch.");
                        }

                        synchronized (HDFSVideoTrackingStream.class) {
                            kvList.parallelStream().forEach(kv -> {
                                try {
                                    final String taskID = kv._1();
                                    final TaskData taskData = kv._2();

                                    final String videoURL = (String) taskData.predecessorRes;
                                    logger.debug("Received taskID=" + taskID + ", URL=" + videoURL);

                                    final Path videoPath = new Path(videoURL);
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
                                    if (!confBuffer.getInst().containsKey(confFile)) {
                                        InputStream confStream = getClass().getResourceAsStream(
                                                "/conf/" + APP_NAME + "/" + confFile);
                                        if (confStream == null) {
                                            throw new IllegalArgumentException(
                                                    "Tracking configuration file not found in JAR!");
                                        }
                                        confBuffer.getInst().put(confFile, IOUtils.toByteArray(confStream));
                                    }
                                    final byte[] confBytes = confBuffer.getInst().get(confFile);
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
                                                final InputStream videoStream = hdfs.open(videoPath);
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
                                            final String videoRoot = metadataDir + "/" + tracklet.id.videoID;
                                            final String taskRoot = videoRoot + "/" + taskID;
                                            final String storeDir = taskRoot + "/" + tracklet.id.serialNumber;
                                            logger.debug("Tracklet " + tracklet.id
                                                    + " is too long. Passing it through HDFS at \"" + storeDir + "\".");
                                            HadoopHelper.storeTracklet(storeDir, tracklet, hdfs);
                                            output(outputPorts,
                                                    taskData.executionPlan,
                                                    new TrackletOrURL(storeDir),
                                                    taskID);
                                        }
                                    }
                                } catch (Throwable e) {
                                    logger.error("During tracking.", e);
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

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }
}
