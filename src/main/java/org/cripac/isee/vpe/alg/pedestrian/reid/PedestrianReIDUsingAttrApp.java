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

package org.cripac.isee.vpe.alg.pedestrian.reid;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.alg.pedestrian.reid.PedestrianReIDer;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianReIDerWithAttr;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;
import scala.Tuple2;

import java.util.*;

/**
 * The PedestrianReIDApp class is a Spark Streaming application which performs
 * pedestrian re-identification with attributes.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianReIDUsingAttrApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-reid-using-attr";
    private static final long serialVersionUID = 7561012713161590005L;

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }

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
         * Port to input pedestrian tracklets from Kafka.
         */
        public static final Port TRACKLET_PORT =
                new Port("pedestrian-tracklet-for-reid-using-attr",
                        DataType.TRACKLET);
        /**
         * Port to input pedestrian attributes from Kafka.
         */
        public static final Port ATTR_PORT =
                new Port("pedestrian-attr-for-reid-using-attr",
                        DataType.ATTRIBUTES);
        /**
         * Port to input pedestrian track with attributes from Kafka.
         */
        public static final Port TRACKLET_ATTR_PORT =
                new Port("pedestrian-track-attr-for-reid-using-attr", DataType.TRACKLET_ATTR);
        private static final long serialVersionUID = 3988152284961510251L;

        /**
         * Duration for buffering results.
         */
        private int bufDuration;

        private Singleton<PedestrianReIDer> reidSingleton;

        public ReIDStream(SystemPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            bufDuration = propCenter.bufDuration;

            reidSingleton = new Singleton<>(FakePedestrianReIDerWithAttr::new, FakePedestrianReIDerWithAttr.class);
        }

        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            final JavaPairDStream<UUID, TaskData> trackletDStream = filter(globalStreamMap, TRACKLET_PORT);
            final JavaPairDStream<UUID, TaskData> attrDStream = filter(globalStreamMap, ATTR_PORT);
            // Read track with attribute bytes in parallel from Kafka.
            // Recover attributes from the bytes and extract the IDRANK of the track the
            // attributes belong to.
            final JavaPairDStream<UUID, TaskData> integralTrackletAttrDStream =
                    filter(globalStreamMap, TRACKLET_ATTR_PORT);

            // Join the track globalStream and attribute globalStream, tolerating failure.
            final JavaPairDStream<UUID, Tuple2<Optional<TaskData>, Optional<TaskData>>>
                    unsurelyJoinedDStream = trackletDStream.fullOuterJoin(attrDStream);

            // Filter out instantly joined pairs.
            final JavaPairDStream<UUID, Tuple2<TaskData, TaskData>> instantlyJoinedDStream =
                    unsurelyJoinedDStream
                            .filter(item -> (Boolean) (item._2()._1().isPresent() && item._2()._2().isPresent()))
                            .mapValues(optPair -> new Tuple2<>(optPair._1().get(), optPair._2().get()));

            // Filter out tracklets that cannot find attributes to match.
            final JavaPairDStream<UUID, TaskData> unjoinedTrackletDStream =
                    unsurelyJoinedDStream
                            .filter(item -> (Boolean) (item._2()._1().isPresent() && !item._2()._2().isPresent()))
                            .mapValues(optPair -> optPair._1().get());

            // Filter out attributes that cannot find tracklets to match.
            final JavaPairDStream<UUID, TaskData> unjoinedAttrStream = unsurelyJoinedDStream
                    .filter(item -> (Boolean) (!item._2()._1().isPresent() && item._2()._2().isPresent()))
                    .mapValues(optPair -> optPair._2().get());

            final JavaPairDStream<UUID, Tuple2<Optional<TaskData>, TaskData>>
                    unsurelyJoinedAttrDStream =
                    unjoinedTrackletDStream
                            .window(Durations.milliseconds(bufDuration))
                            .rightOuterJoin(unjoinedAttrStream);

            final JavaPairDStream<UUID, Tuple2<TaskData, TaskData>> lateAttrJoinedDStream =
                    unsurelyJoinedAttrDStream
                            .filter(item -> (Boolean) (item._2()._1().isPresent()))
                            .mapValues(item -> new Tuple2<>(item._1().get(), item._2()));

            final JavaPairDStream<UUID, Tuple2<TaskData, TaskData>> lateTrackletJoinedDStream =
                    unjoinedTrackletDStream
                            .join(unsurelyJoinedAttrDStream
                                    .filter(item -> (Boolean) (!item._2()._1().isPresent()))
                                    .mapValues(Tuple2::_2)
                                    .window(Durations.milliseconds(bufDuration)));

            // Union the three track and attribute streams and assemble
            // their TaskData.
            final JavaPairDStream<UUID, TaskData> asmTrackletAttrDStream =
                    instantlyJoinedDStream.union(lateTrackletJoinedDStream)
                            .union(lateAttrJoinedDStream)
                            .mapToPair(pack -> {
                                UUID taskID = pack._1();
                                TaskData taskDataWithTracklet = pack._2()._1();
                                TaskData taskDataWithAttr = pack._2()._2();

                                taskDataWithTracklet.executionPlan.combine(taskDataWithAttr.executionPlan);
                                TaskData asmTaskData = new TaskData(
                                        taskDataWithTracklet.destPorts.values(),
                                        taskDataWithTracklet.executionPlan,
                                        new PedestrianInfo(
                                                (TrackletOrURL) taskDataWithTracklet.predecessorRes,
                                                (Attributes) taskDataWithAttr.predecessorRes));
                                loggerSingleton.getInst().debug(
                                        "Assembled track and attr of " + pack._1());
                                return new Tuple2<>(taskID, asmTaskData);
                            });

            // Union the two track with attribute streams and perform ReID.
            integralTrackletAttrDStream.union(asmTrackletAttrDStream)
                    .foreachRDD(rdd -> rdd.foreach(kv -> {
                        final Logger logger = loggerSingleton.getInst();
                        try {
                            UUID taskID = kv._1();
                            final TaskData taskData = kv._2();
                            final PedestrianInfo trackletWithAttr = (PedestrianInfo) taskData.predecessorRes;

                            // Perform ReID.
                            final int[] idRank = new RobustExecutor<Void, int[]>(
                                    (Function0<int[]>) () -> reidSingleton.getInst().reid(trackletWithAttr)
                            ).execute();

                            // Find current node.
                            final TaskData.ExecutionPlan.Node curNode = taskData.getDestNode(getPorts());
                            // Get ports to output to.
                            final List<TaskData.ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                            // Mark the current node as executed in advance.
                            curNode.markExecuted();

                            // Send to all the successor nodes.
                            output(outputPorts, taskData.executionPlan, idRank, taskID);
                        } catch (Exception e) {
                            logger.error("During ReID", e);
                        }
                    }));
        }

        @Override
        public List<Port> getPorts() {
            return Arrays.asList(TRACKLET_PORT, ATTR_PORT, TRACKLET_ATTR_PORT);
        }
    }
}
