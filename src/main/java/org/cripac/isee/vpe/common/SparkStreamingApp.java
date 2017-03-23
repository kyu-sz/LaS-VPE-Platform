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

package org.cripac.isee.vpe.common;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.cripac.isee.vpe.ctrl.MonitorThread;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskController;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.Tuple2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;

/**
 * The SparkStreamingApp class wraps a whole Spark Streaming application,
 * including driver code and executor code. After initialized, it can be used
 * just like a JavaStreamingContext class. Note that you must call the
 * initialize() method after construction and before using it.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class SparkStreamingApp implements Serializable {

    private static final long serialVersionUID = 3098753124157119358L;
    @Nonnull
    private SystemPropertyCenter propCenter;
    @Nonnull
    private final String appName;

    /**
     * Kafka parameters for creating input streams pulling messages from Kafka
     * Brokers.
     */
    @Nonnull
    private final Map<String, Object> kafkaParams;

    public SparkStreamingApp(@Nonnull SystemPropertyCenter propCenter,
                             @Nonnull String appName) throws Exception {
        this.propCenter = propCenter;
        this.appName = appName;
        this.kafkaParams = propCenter.getKafkaParams(appName);
        this.loggerSingleton = new Singleton<>(
                new SynthesizedLoggerFactory(appName, propCenter),
                SynthesizedLogger.class);
        this.monitorSingleton = new Singleton<>(() -> {
            MonitorThread monitorThread = new MonitorThread(loggerSingleton.getInst());
            monitorThread.start();
            return monitorThread;
        }, MonitorThread.class);
        if (propCenter.taskControllerEnable) {
            this.taskController = new Singleton<>(() -> {
                TaskController taskController = new TaskController(propCenter, loggerSingleton.getInst());
                taskController.start();
                return taskController;
            }, TaskController.class);
        }
    }

    /**
     * Common Spark Streaming context variable.
     */
    private transient JavaStreamingContext jssc = null;

    @Nonnull
    protected final Singleton<Logger> loggerSingleton;

    @Nonnull
    private final List<Stream> streams = new ArrayList<>();

    @Nonnull
    private final Singleton<MonitorThread> monitorSingleton;

    @Nullable
    private Singleton<TaskController> taskController = null;

    protected void registerStreams(Collection<Stream> streams) {
        this.streams.addAll(streams);
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param acceptingTypes Data types the stream accepts.
     * @param repartition    Number of partitions when repartitioning the RDDs.
     *                       -1 means do not do repartition. 0 means using default parallelism of Spark.
     * @return A Kafka non-receiver input stream.
     */
    @Nonnull
    protected JavaPairDStream<DataType, Tuple2<String, byte[]>>
    buildDirectStream(@Nonnull Collection<DataType> acceptingTypes,
                      int repartition) throws SparkException {
        final JavaInputDStream<ConsumerRecord<String, byte[]>> inputDStream =
                KafkaUtils.createDirectStream(jssc,
                        propCenter.kafkaLocationStrategy.equals("PreferBrokers") ?
                                LocationStrategies.PreferBrokers() :
                                LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(
                                acceptingTypes.stream().map(Enum::name).collect(Collectors.toList()),
                                kafkaParams));

        JavaPairDStream<DataType, Tuple2<String, byte[]>> stream = inputDStream
                // Manipulate offsets.
                .transform(rdd -> {
                    final Logger logger = loggerSingleton.getInst();
                    if (!monitorSingleton.getInst().isAlive()) {
                        logger.error("Monitor is dead!");
                    }

                    // Store offsets.
                    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                    // Directly commit the offsets, since data has been checkpointed in Spark Streaming.
                    ((CanCommitOffsets) inputDStream.inputDStream()).commitAsync(offsetRanges);

                    // Find offsets which indicate new messages have been received.
                    rdd.foreachPartition(consumerRecords -> {
                        final Logger executorLogger = loggerSingleton.getInst();
                        if (!monitorSingleton.getInst().isAlive()) {
                            executorLogger.error("Monitor is dead!");
                        }
                        final OffsetRange o = offsetRanges[TaskContext.getPartitionId()];
                        if (o.fromOffset() < o.untilOffset()) {
                            executorLogger.debug("Received {topic=" + o.topic()
                                    + ", partition=" + o.partition()
                                    + ", fromOffset=" + o.fromOffset()
                                    + ", untilOffset=" + o.untilOffset() + "}");
                        }
                    });
                    int numNewMessages = 0;
                    for (OffsetRange o : offsetRanges) {
                        numNewMessages += o.untilOffset() - o.fromOffset();
                    }
                    if (numNewMessages == 0) {
                        logger.debug("No new messages!");
                    } else {
                        logger.debug("Received " + numNewMessages + " messages totally.");
                    }
                    return rdd;
                })
                .mapToPair(rec -> new Tuple2<>(DataType.valueOf(rec.topic()),
                        new Tuple2<>(rec.key(), rec.value())));

        if (repartition >= 0) {
            // Repartition the records.
            stream = stream.repartition(repartition == 0 ? jssc.sparkContext().defaultParallelism() : repartition);
        }

        return stream;
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream. RDDs are repartitioned by default.
     *
     * @param acceptingTypes Data types the stream accepts.
     * @return A Kafka non-receiver input stream.
     */
    @Nonnull
    protected JavaPairDStream<DataType, Tuple2<String, byte[]>>
    buildDirectStream(@Nonnull Collection<DataType> acceptingTypes) throws SparkException {
        return buildDirectStream(acceptingTypes, propCenter.repartition);
    }

    /**
     * Add streaming actions directly to the global streaming context.
     * This is for applications that may take as input messages with types other than {@link TaskData}.
     * Actions that take {@link TaskData} as input should be implemented in the
     * {@link Stream#addToGlobalStream(Map)}, in order to save time of deserialization.
     * Note that existence of Kafka topics used in this method is not automatically checked.
     */
    public abstract void addToContext() throws Exception;

    /**
     * Initialize the application.
     */
    public void initialize() {
        KafkaHelper.checkTopics(propCenter.zkConn,
                propCenter.zkSessionTimeoutMs,
                propCenter.zkConnectionTimeoutMS,
                propCenter.kafkaNumPartitions,
                propCenter.kafkaReplFactor);

        final Collection<DataType> acceptingTypes = streams.stream()
                .flatMap(stream -> stream.getPorts().stream().map(port -> port.inputType))
                .collect(Collectors.toList());

        String checkpointDir = propCenter.checkpointRootDir + "/" + appName;
        jssc = JavaStreamingContext.getOrCreate(checkpointDir, () -> {
            // Load default Spark configurations.
            SparkConf sparkConf = new SparkConf(true)
                    .set("spark.executor.memory", propCenter.executorMem)
                    .set("spark.executor.instances", "" + propCenter.numExecutors)
                    // Register custom classes with Kryo.
                    .registerKryoClasses(new Class[]{TaskData.class, DataType.class});
            if (propCenter.maxRatePerPartition != null) {
                // Set maximum number of messages per second that each partition will accept
                // in the direct Kafka input stream.
                sparkConf = sparkConf
                        .set("spark.streaming.kafka.maxRatePerPartition", "" + propCenter.maxRatePerPartition);
            }
            // Create contexts.
            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
            jsc.setLocalProperty("spark.scheduler.pool", "vpe");
            jssc = new JavaStreamingContext(jsc, Durations.milliseconds(propCenter.batchDuration));

            addToContext();

            if (!acceptingTypes.isEmpty()) {
                JavaPairDStream<DataType, Tuple2<UUID, byte[]>> inputStream =
                        buildDirectStream(acceptingTypes)
                                .mapValues(tuple -> new Tuple2<>(UUID.fromString(tuple._1()), tuple._2()));
                if (taskController != null) {
                    inputStream = inputStream.filter(kv ->
                            (Boolean) !taskController.getInst().termSigPool.contains(kv._2()._1()));
                }
                Map<DataType, JavaPairDStream<UUID, TaskData>> streamMap = new Object2ObjectOpenHashMap<>();
                for (DataType type : acceptingTypes) {
                    streamMap.put(type,
                            inputStream.filter(rec -> (Boolean) (Objects.equals(rec._1(), type)))
                                    .mapToPair(rec -> new Tuple2<>(rec._2()._1(), deserialize(rec._2()._2()))));
                }
                streams.forEach(stream -> stream.addToGlobalStream(streamMap));
            }

            try {
                if (propCenter.sparkMaster.contains("local")) {
                    File dir = new File(checkpointDir);
                    //noinspection ResultOfMethodCallIgnored
                    dir.delete();
                    //noinspection ResultOfMethodCallIgnored
                    dir.mkdirs();
                } else {
                    FileSystem fs = FileSystem.get(new Configuration());
                    Path dir = new Path(checkpointDir);
                    fs.delete(dir, true);
                    fs.mkdirs(dir);
                }
                jssc.checkpoint(checkpointDir);
            } catch (IllegalArgumentException | IOException e) {
                e.printStackTrace();
            }
            return jssc;
        }, new Configuration(), true);
    }

    /**
     * Start the application.
     */
    public void start() {
        jssc.start();
    }

    /**
     * Stop the application.
     */
    public void stop() {
        jssc.stop();
    }

    /**
     * Await termination of the application.
     */
    public void awaitTermination() {
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (jssc != null) {
            jssc.close();
        }
        super.finalize();
    }
}
