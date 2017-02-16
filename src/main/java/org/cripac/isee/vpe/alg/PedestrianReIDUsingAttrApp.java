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
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.pedestrian.reid.PedestrianReIDer;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.common.Topic;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianReIDerWithAttr;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

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
    private static final long serialVersionUID = 7561012713161590005L;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception On failure in Spark.
     */
    public PedestrianReIDUsingAttrApp(SystemPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new ReIDStream(propCenter)));
    }

    /**
     * @param args No options supported currently.
     * @throws Exception On failure in Spark.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianReIDUsingAttrApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class ReIDStream extends Stream {

        public static final String NAME = "PedestrianReIDUsingAttr";
        public static final DataType OUTPUT_TYPE = DataType.IDRANK;

        /**
         * Topic to input pedestrian tracklets from Kafka.
         */
        public static final Topic TRACKLET_TOPIC =
                new Topic("pedestrian-tracklet-for-reid-using-attr",
                        DataType.TRACKLET);
        /**
         * Topic to input pedestrian attributes from Kafka.
         */
        public static final Topic ATTR_TOPIC =
                new Topic("pedestrian-attr-for-reid-using-attr",
                        DataType.ATTR);
        /**
         * Topic to input pedestrian track with attributes from Kafka.
         */
        public static final Topic TRACKLET_ATTR_TOPIC =
                new Topic("pedestrian-track-attr-for-reid-using-attr", DataType.TRACKLET_ATTR);
        private static final long serialVersionUID = 3988152284961510251L;

        /**
         * Duration for buffering results.
         */
        private int bufDuration;

        private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private Singleton<PedestrianReIDer> reidSingleton;

        public ReIDStream(SystemPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            bufDuration = propCenter.bufDuration;

            Properties producerProp = propCenter.getKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));

            reidSingleton = new Singleton<>(FakePedestrianReIDerWithAttr::new);
        }

        @Override
        public void addToStream(JavaDStream<StringByteArrayRecord> globalStream) {
            final JavaPairDStream<String, TaskData<Tracklet>> trackletDStream =
                    filter(globalStream, TRACKLET_TOPIC);
            final JavaPairDStream<String, TaskData<Attributes>> attrDStream =
                    filter(globalStream, ATTR_TOPIC);
            // Read track with attribute bytes in parallel from Kafka.
            // Recover attributes from the bytes and extract the IDRANK of the track the
            // attributes belong to.
            final JavaPairDStream<String, TaskData<PedestrianInfo>> integralTrackletAttrDStream =
                    filter(globalStream, TRACKLET_ATTR_TOPIC);

            // Join the track globalStream and attribute globalStream, tolerating failure.
            final JavaPairDStream<String, Tuple2<Optional<TaskData<Tracklet>>, Optional<TaskData<Attributes>>>>
                    unsurelyJoinedDStream = trackletDStream.fullOuterJoin(attrDStream);

            // Filter out instantly joined pairs.
            final JavaPairDStream<String, Tuple2<TaskData<Tracklet>, TaskData<Attributes>>> instantlyJoinedDStream =
                    unsurelyJoinedDStream
                            .filter(item -> (Boolean) (item._2()._1().isPresent() && item._2()._2().isPresent()))
                            .mapValues(optPair -> new Tuple2<>(optPair._1().get(), optPair._2().get()));

            // Filter out tracklets that cannot find attributes to match.
            final JavaPairDStream<String, TaskData<Tracklet>> unjoinedTrackletDStream =
                    unsurelyJoinedDStream
                            .filter(item -> (Boolean) (item._2()._1().isPresent() && !item._2()._2().isPresent()))
                            .mapValues(optPair -> optPair._1().get());

            // Filter out attributes that cannot find tracklets to match.
            final JavaPairDStream<String, TaskData<Attributes>> unjoinedAttrStream = unsurelyJoinedDStream
                    .filter(item -> (Boolean) (!item._2()._1().isPresent() && item._2()._2().isPresent()))
                    .mapValues(optPair -> optPair._2().get());

            final JavaPairDStream<String, Tuple2<Optional<TaskData<Tracklet>>, TaskData<Attributes>>>
                    unsurelyJoinedAttrDStream =
                    unjoinedTrackletDStream
                            .window(Durations.milliseconds(bufDuration))
                            .rightOuterJoin(unjoinedAttrStream);

            final JavaPairDStream<String, Tuple2<TaskData<Tracklet>, TaskData<Attributes>>> lateAttrJoinedDStream =
                    unsurelyJoinedAttrDStream
                            .filter(item -> (Boolean) (item._2()._1().isPresent()))
                            .mapValues(item -> new Tuple2<>(item._1().get(), item._2()));

            final JavaPairDStream<String, Tuple2<TaskData<Tracklet>, TaskData<Attributes>>> lateTrackJoinedDStream =
                    unjoinedTrackletDStream
                            .join(unsurelyJoinedAttrDStream
                                    .filter(item -> (Boolean) (!item._2()._1().isPresent()))
                                    .mapValues(Tuple2::_2)
                                    .window(Durations.milliseconds(bufDuration)));

            // Union the three track and attribute streams and assemble
            // their TaskData.
            final JavaPairDStream<String, TaskData<PedestrianInfo>> asmTrackletAttrDStream =
                    instantlyJoinedDStream.union(lateTrackJoinedDStream)
                            .union(lateAttrJoinedDStream)
                            .mapToPair(pack -> {
                                String taskID = pack._1().split(":")[0];
                                TaskData taskDataWithTrack = pack._2()._1();
                                TaskData taskDataWithAttr = pack._2()._2();

                                taskDataWithTrack.executionPlan.combine(taskDataWithAttr.executionPlan);

                                TaskData<PedestrianInfo> asmTaskData = new TaskData<>(
                                        taskDataWithTrack.curNode,
                                        taskDataWithTrack.executionPlan,
                                        new PedestrianInfo(
                                                (Tracklet) taskDataWithTrack.predecessorRes,
                                                (Attributes) taskDataWithAttr.predecessorRes));
                                loggerSingleton.getInst().debug(
                                        "Assembled track and attr of " + pack._1());
                                return new Tuple2<>(taskID, asmTaskData);
                            });

            // Union the two track with attribute streams and perform ReID.
            integralTrackletAttrDStream.union(asmTrackletAttrDStream)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        try {
                            Logger logger = loggerSingleton.getInst();
                            String taskID = kv._1();
                            final TaskData taskData = kv._2();
                            PedestrianInfo trackletWithAttr = (PedestrianInfo) taskData.predecessorRes;

                            // Perform ReID.
                            final int[] res = reidSingleton.getInst().reid(trackletWithAttr);
                            // Get the IDs of successor nodes.
                            List<TaskData.ExecutionPlan.Node.Port> outputPorts = taskData.curNode.getOutputPorts();
                            // Mark the current node as executed.
                            taskData.curNode.markExecuted();

                            // Send to all the successor nodes.
                            final KafkaProducer<String, byte[]> producer = producerSingleton.getInst();
                            for (TaskData.ExecutionPlan.Node.Port port : outputPorts) {
                                final TaskData<int[]> resTaskData = new TaskData<>(
                                        port.getNode(), taskData.executionPlan, res
                                );

                                final byte[] serialized = serialize(resTaskData);
                                sendWithLog(port.topic, taskID, serialized, producer, logger);
                            }
                        } catch (Exception e) {
                            loggerSingleton.getInst().error("During ReID", e);
                        }
                    }));
            ;
        }

        @Override
        public List<String> listeningTopics() {
            return Arrays.asList(TRACKLET_TOPIC.NAME, ATTR_TOPIC.NAME);
        }
    }
}
