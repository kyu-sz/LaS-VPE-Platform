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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.pedestrian.reid.PedestrianReIDer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.debug.FakePedestrianReIDerWithAttr;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.Tuple2;

import java.util.*;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The PedestrianReIDApp class is a Spark Streaming application which performs
 * pedestrian re-identification with attributes.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianReIDUsingAttrApp extends SparkStreamingApp {
    /**
     * The NAME of this application.
     */
    public static final String APP_NAME = "pedestrian-reID-using-attr";
    private int batchDuration = 1000;

    private Stream reidStream;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception
     */
    public PedestrianReIDUsingAttrApp(SystemPropertyCenter propCenter) throws Exception {
        batchDuration = propCenter.batchDuration;
        reidStream = new ReIDStream(propCenter);
    }

    /**
     * @param args No options supported currently.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        SystemPropertyCenter propertyCenter;
        propertyCenter = new SystemPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianReIDUsingAttrApp(propertyCenter);
        TopicManager.checkTopics(propertyCenter);
        app.initialize(propertyCenter);
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
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf(true));
        sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, Durations.milliseconds(batchDuration));

        reidStream.addToContext(jsc);

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

    public static class ReIDStream extends Stream {

        public static final Info INFO =
                new Info("PedestrianReIDUsingAttr", DataTypes.IDRANK);

        /**
         * Topic to input pedestrian tracklets from Kafka.
         */
        public static final Topic TRACKLET_TOPIC =
                new Topic("pedestrian-tracklet-for-reid-using-attr",
                        DataTypes.TRACKLET, INFO);
        /**
         * Topic to input pedestrian attributes from Kafka.
         */
        public static final Topic ATTR_TOPIC =
                new Topic("pedestrian-attr-for-reid-using-attr",
                        DataTypes.ATTR, INFO);
        /**
         * Topic to input pedestrian track with attributes from Kafka.
         */
        public static final Topic TRACKLET_ATTR_TOPIC =
                new Topic("pedestrian-track-attr-for-reid-using-attr",
                        DataTypes.TRACKLET_ATTR, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages from Kafka
         * Brokers.
         */
        private Map<String, String> kafkaParams = new HashMap<>();

        /**
         * Duration for buffering results.
         */
        private int bufDuration;

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<PedestrianReIDer> reidSingleton;

        public ReIDStream(SystemPropertyCenter propCenter) throws Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            bufDuration = propCenter.bufDuration;

            // Common kafka settings.
            kafkaParams = propCenter.generateKafkaParams(INFO.NAME);

            Properties producerProp = propCenter.generateKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));

            reidSingleton = new Singleton<>(() -> new FakePedestrianReIDerWithAttr());
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            JavaPairDStream<String, TaskData> trackletDStream =
                    // Read track bytes in parallel from Kafka.
                    buildBytesDirectStream(jssc, Arrays.asList(TRACKLET_TOPIC.NAME), kafkaParams)
                            // Recover track from the bytes
                            // and extract the IDRANK of the track.
                            .mapToPair(kvPair -> {
                                TaskData taskData;
                                try {
                                    taskData = (TaskData) deserialize(kvPair._2());
                                } catch (Exception e) {
                                    loggerSingleton.getInst().error("During TaskData deserialization", e);
                                    return null;
                                }
                                loggerSingleton.getInst().info(
                                        "Received track: " + ((Tracklet) taskData.predecessorRes).id);
                                return new Tuple2<>(
                                        kvPair._1() + ":" + ((Tracklet) taskData.predecessorRes).id,
                                        taskData);
                            });

            JavaPairDStream<String, TaskData> attrDStream =
                    // Read attribute bytes in parallel from Kafka.
                    buildBytesDirectStream(jssc, Arrays.asList(ATTR_TOPIC.NAME), kafkaParams)
                            // Recover attributes from the bytes
                            // and extract the IDRANK of the track
                            // the attributes belong to.
                            .mapToPair(kvPair -> {
                                TaskData taskData;
                                try {
                                    taskData = (TaskData) deserialize(kvPair._2());
                                } catch (Exception e) {
                                    loggerSingleton.getInst().error("During TaskData deserialization", e);
                                    return null;
                                }

                                if (!(taskData.predecessorRes instanceof Attributes)) {
                                    throw new DataTypeNotMatchedException(
                                            "Predecessor result is expected to be attributes,"
                                                    + " but received \""
                                                    + taskData.predecessorRes
                                                    + "\"!");
                                }

                                loggerSingleton.getInst().info(
                                        "Received " + kvPair._1() + ": " + taskData);
                                return new Tuple2<>(kvPair._1() + ":"
                                        + ((Attributes) taskData.predecessorRes).trackletID,
                                        taskData);
                            });

            // Join the track stream and attribute stream, tolerating failure.
            JavaPairDStream<String, Tuple2<Optional<TaskData>, Optional<TaskData>>> unsurelyJoinedDStream =
                    trackletDStream.fullOuterJoin(attrDStream);

            // Filter out instantly joined pairs.
            JavaPairDStream<String, Tuple2<TaskData, TaskData>> instantlyJoinedDStream =
                    unsurelyJoinedDStream
                            .filter(item -> new Boolean(item._2()._1().isPresent() && item._2()._2().isPresent()))
                            .mapValues(optPair -> new Tuple2<>(optPair._1().get(), optPair._2().get()));

            // Filter out tracklets that cannot find attributes to match.
            JavaPairDStream<String, TaskData> unjoinedTrackDStream =
                    unsurelyJoinedDStream
                            .filter(item -> new Boolean(item._2()._1().isPresent() && !item._2()._2().isPresent()))
                            .mapValues(optPair -> optPair._1().get());

            // Filter out attributes that cannot find tracklets to match.
            JavaPairDStream<String, TaskData> unjoinedAttrStream = unsurelyJoinedDStream
                    .filter(item -> new Boolean(!item._2()._1().isPresent() && item._2()._2().isPresent()))
                    .mapValues(optPair -> optPair._2().get());

            JavaPairDStream<String, Tuple2<Optional<TaskData>, TaskData>> unsurelyJoinedAttrDStream =
                    unjoinedTrackDStream
                            .window(Durations.milliseconds(bufDuration))
                            .rightOuterJoin(unjoinedAttrStream);

            JavaPairDStream<String, Tuple2<TaskData, TaskData>> lateAttrJoinedDStream =
                    unsurelyJoinedAttrDStream
                            .filter(item -> new Boolean(item._2()._1().isPresent()))
                            .mapValues(item -> new Tuple2<>(item._1().get(), item._2()));

            JavaPairDStream<String, Tuple2<TaskData, TaskData>> lateTrackJoinedDStream =
                    unjoinedTrackDStream
                            .join(unsurelyJoinedAttrDStream
                                    .filter(item -> new Boolean(!item._2()._1().isPresent()))
                                    .mapValues(item -> item._2())
                                    .window(Durations.milliseconds(bufDuration)));

            // Union the three track and attribute streams and assemble
            // their TaskData.
            JavaPairDStream<String, TaskData> asmTrackletAttrDStream =
                    instantlyJoinedDStream.union(lateTrackJoinedDStream)
                            .union(lateAttrJoinedDStream)
                            .mapToPair(pack -> {
                                String taskID = pack._1().split(":")[0];
                                TaskData taskDataWithTrack = pack._2()._1();
                                TaskData taskDataWithAttr = pack._2()._2();
                                TaskData.ExecutionPlan asmPlan =
                                        TaskData.ExecutionPlan.combine(
                                                taskDataWithTrack.executionPlan,
                                                taskDataWithAttr.executionPlan);

                                TaskData asmTaskData = new TaskData(
                                        taskDataWithTrack.curNode,
                                        asmPlan,
                                        new PedestrianInfo(
                                                (Tracklet) taskDataWithTrack.predecessorRes,
                                                (Attributes) taskDataWithAttr.predecessorRes));
                                loggerSingleton.getInst().debug(
                                        "Assembled track and attr of " + pack._1());
                                return new Tuple2<>(taskID, asmTaskData);
                            });

            // Read track with attribute bytes in parallel from Kafka.
            // Recover attributes from the bytes and extract the IDRANK of the track the
            // attributes belong to.
            JavaPairDStream<String, TaskData> integralTrackletAttrDStream =
                    buildBytesDirectStream(jssc, Arrays.asList(TRACKLET_ATTR_TOPIC.NAME), kafkaParams)
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

            // Union the two track with attribute streams and perform ReID.
            integralTrackletAttrDStream.union(asmTrackletAttrDStream)
                    .foreachRDD(rdd -> {
                        rdd.foreach(taskWithTrackletAttr -> {
                            try {
                                Logger logger = loggerSingleton.getInst();
                                String taskID = taskWithTrackletAttr._1();
                                TaskData taskData = taskWithTrackletAttr._2();
                                if (taskData.predecessorRes == null) {
                                    throw new DataTypeNotMatchedException("TaskData from " + taskData.predecessorInfo
                                            + " contains no result data!");
                                }
                                if (!(taskData.predecessorRes instanceof PedestrianInfo)) {
                                    throw new DataTypeNotMatchedException("Result sent by "
                                            + taskData.predecessorInfo
                                            + " is expected to be a PedestrianInfo,"
                                            + " but received \""
                                            + taskData.predecessorRes + "\"!");
                                }
                                PedestrianInfo trackletWithAttr =
                                        (PedestrianInfo) taskData.predecessorRes;

                                // Perform ReID.
                                int[] idRank = reidSingleton.getInst().reid(trackletWithAttr);

                                // Prepare new task data with the pedestrian IDRANK.
                                taskData.predecessorRes = idRank;
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
                                            taskID,
                                            serialize(taskData),
                                            producerSingleton.getInst(),
                                            logger);
                                }
                            } catch (Exception e) {
                                loggerSingleton.getInst().error("During ReID", e);
                            }
                        });
                    });
        }
    }
}
