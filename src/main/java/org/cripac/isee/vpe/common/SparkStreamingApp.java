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

package org.cripac.isee.vpe.common;

import kafka.admin.AdminUtils;
import kafka.common.TopicAndPartition;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.collection.JavaConversions;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    private SystemPropertyCenter propCenter;
    private final String appName;

    /**
     * Kafka parameters for creating input streams pulling messages from Kafka
     * Brokers.
     */
    private final Map<String, String> kafkaParams;

    public SparkStreamingApp(SystemPropertyCenter propCenter, String appName) throws Exception {
        this.propCenter = propCenter;
        this.appName = appName;
        this.loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(appName, propCenter));
        kafkaParams = propCenter.getKafkaParams(appName);
    }

    /**
     * Common Spark Streaming context variable.
     */
    private transient JavaStreamingContext jssc = null;

    protected final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

    protected final Singleton<Logger> loggerSingleton;

    private final List<Stream> streams = new ArrayList<>();

    private void checkTopics(Collection<String> topics) {
        Logger logger;
        try {
            logger = loggerSingleton.getInst();
        } catch (Exception e) {
            e.printStackTrace();
            logger = new ConsoleLogger(Level.DEBUG);
        }
        logger.info("Checking topics: " + topics.stream().reduce("", (s1, s2) -> s1 + ", " + s2));

        logger.info("Connecting to zookeeper: " + propCenter.zkConn);
        ZkConnection zkConn = new ZkConnection(propCenter.zkConn, propCenter.sessionTimeoutMs);
        ZkClient zkClient = new ZkClient(zkConn);

        for (String topic : topics) {
            if (!AdminUtils.topicExists(zkClient, topic)) {
                // AdminUtils.createTopic(zkClient, topic,
                // propCenter.kafkaNumPartitions,
                // propCenter.kafkaReplFactor, new Properties());
                logger.info("Creating topic: " + topic);
                kafka.admin.TopicCommand.main(
                        new String[]{
                                "--create",
                                "--zookeeper", propCenter.zkConn,
                                "--topic", topic,
                                "--partitions", "" + propCenter.kafkaNumPartitions,
                                "--replication-factor", "" + propCenter.kafkaReplFactor});
            }
        }

        logger.info("Topics checked!");
    }

    protected void registerStreams(Collection<Stream> streams) {
        this.streams.addAll(streams);
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param topics        Topics from which the direct stream reads.
     * @param toRepartition Whether to repartition the RDDs.
     * @return A Kafka non-receiver input stream.
     */
    protected JavaDStream<StringByteArrayRecord>
    buildDirectStream(@Nonnull Collection<String> topics,
                      boolean toRepartition) throws SparkException {
        final KafkaCluster kafkaCluster = KafkaHelper.createKafkaCluster(kafkaParams);

        Logger tmpLogger;
        try {
            tmpLogger = loggerSingleton.getInst();
        } catch (Exception e) {
            tmpLogger = new ConsoleLogger();
            e.printStackTrace();
        }
        tmpLogger.info("Getting initial fromOffsets from Kafka cluster.");
        // Retrieve and correct offsets from Kafka cluster.
        final Map<TopicAndPartition, Long> fromOffsets = KafkaHelper.getFromOffsets(kafkaCluster, topics);
        tmpLogger.info("Initial fromOffsets=" + fromOffsets);

        // Create a direct stream from the retrieved offsets.
        JavaDStream<StringByteArrayRecord> stream = KafkaUtils.createDirectStream(
                jssc,
                String.class, byte[].class,
                StringDecoder.class, DefaultDecoder.class,
                StringByteArrayRecord.class,
                JavaConversions.mapAsJavaMap(kafkaCluster.kafkaParams()),
                fromOffsets,
                mmd -> new StringByteArrayRecord(mmd.key(), mmd.message(), mmd.topic()))
                // Manipulate offsets.
                .transform(rdd -> {
                    final Logger logger = loggerSingleton.getInst();

                    // Store offsets.
                    final OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    offsetRanges.set(offsets);

                    // Directly commit the offsets, since data has been checkpointed in Spark Streaming.
                    KafkaHelper.submitOffset(kafkaCluster, offsetRanges.get());

                    // Find offsets which indicate new messages have been received.
                    int numNewMessages = 0;
                    for (OffsetRange o : offsets) {
                        if (o.untilOffset() > o.fromOffset()) {
                            numNewMessages += o.untilOffset() - o.fromOffset();
                            logger.debug("Received {topic=" + o.topic()
                                    + ", partition=" + o.partition()
                                    + ", fromOffset=" + o.fromOffset()
                                    + ", untilOffset=" + o.untilOffset() + "}");
                        }
                    }
                    if (numNewMessages == 0) {
                        logger.debug("No new messages!");
                    } else {
                        logger.debug("Received " + numNewMessages + " messages totally.");
                    }
                    return rdd;
                });

        if (toRepartition) {
            // Repartition the records.
            stream = stream.repartition(jssc.sparkContext().defaultParallelism());
        }

        return stream;
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream. RDDs are repartitioned by default.
     *
     * @param topics Topics from which the direct stream reads.
     * @return A Kafka non-receiver input stream.
     */
    protected JavaDStream<StringByteArrayRecord>
    buildDirectStream(@Nonnull Collection<String> topics) throws SparkException {
        return buildDirectStream(topics, true);
    }

    /**
     * Initialize the application.
     */
    public void initialize() {
        final Collection<String> listeningTopics = streams.stream()
                .flatMap(stream -> stream.listeningTopics().stream())
                .collect(Collectors.toList());

        checkTopics(listeningTopics);

        String checkpointDir = propCenter.checkpointRootDir + "/" + appName;
        jssc = JavaStreamingContext.getOrCreate(checkpointDir, () -> {
            // Create contexts.
            JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf(true));
            sparkContext.setLocalProperty("spark.scheduler.pool", "vpe");

            jssc = new JavaStreamingContext(sparkContext, Durations.milliseconds(propCenter.batchDuration));

            final JavaDStream<StringByteArrayRecord> inputStream = buildDirectStream(listeningTopics);
            streams.forEach(stream -> stream.addToStream(inputStream));

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

    public static class StringByteArrayRecord implements Serializable {
        private static final long serialVersionUID = -7522425828162991655L;
        public String key;
        public byte[] value;
        public String topic;

        StringByteArrayRecord(String key, byte[] value, String topic) {
            this.key = key;
            this.value = value;
            this.topic = topic;
        }
    }

}
