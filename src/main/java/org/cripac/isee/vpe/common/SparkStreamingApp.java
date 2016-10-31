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

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The SparkStreamingApp class wraps a whole Spark Streaming application,
 * including driver code and executor code. After initialized, it can be used
 * just like a JavaStreamingContext class. Note that you must call the
 * initialize() method after construction and before using it.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class SparkStreamingApp implements Serializable {

    private static final long serialVersionUID = 2780614096112566164L;

    /**
     * Common Spark Streaming context variable.
     */
    private transient JavaStreamingContext streamingContext = null;

    /**
     * Utilization function for all applications to receive messages with byte
     * array values from Kafka in parallel.
     *
     * @param streamingContext      The streaming context of the applications.
     * @param numRecvStreams        Number of streams to use for parallelization.
     * @param kafkaParams           Parameters for reading from Kafka.
     * @param numPartitionsPerTopic A map specifying topics to read from, each assigned number of
     *                              partitions for the topic.
     * @return A paralleled Kafka receiver input stream.
     */
    protected static JavaPairDStream<String, byte[]>
    buildBytesParRecvStream(JavaStreamingContext streamingContext,
                            int numRecvStreams,
                            Map<String, String> kafkaParams,
                            Map<String, Integer> numPartitionsPerTopic) {
        // Read bytes in parallel from Kafka.
        List<JavaPairDStream<String, byte[]>> parStreams = new ArrayList<>(numRecvStreams);
        for (int i = 0; i < numRecvStreams; i++) {
            parStreams.add(KafkaUtils.createStream(streamingContext, String.class, byte[].class, StringDecoder.class,
                    DefaultDecoder.class, kafkaParams, numPartitionsPerTopic, StorageLevel.MEMORY_AND_DISK_SER()));
        }
        // Union the parallel bytes streams.
        return streamingContext.union(parStreams.get(0), parStreams.subList(1, parStreams.size()));
    }

    /**
     * Utilization function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param streamingContext      The streaming context of the applications.
     * @param kafkaParams           Parameters for reading from Kafka.
     * @param numPartitionsPerTopic A map specifying topics to read from, each assigned number of
     *                              partitions for the topic.
     * @return A Kafka non-receiver input stream.
     */
    protected static JavaPairDStream<String, byte[]>
    buildBytesDirectStream(JavaStreamingContext streamingContext,
                           Map<String, String> kafkaParams,
                           Map<String, Integer> numPartitionsPerTopic) {
        return KafkaUtils
                .createDirectStream(
                        streamingContext,
                        String.class, byte[].class,
                        StringDecoder.class, DefaultDecoder.class,
                        kafkaParams,
                        numPartitionsPerTopic.keySet())
                .transformToPair(rdd -> {
                    rdd.context().setLocalProperty("spark.scheduler.pool", "vpe");
                    return rdd;
                });
    }

    /**
     * Implemented by subclasses, this method produces an application-specified
     * Spark Streaming context.
     *
     * @return An application-specified Spark Streaming context.
     */
    protected abstract JavaStreamingContext getStreamContext();

    abstract public String getAppName();

    /**
     * Initialize the application.
     *
     * @param propCenter Properties of the whole system.
     */
    public void initialize(SystemPropertyCenter propCenter) {
        SynthesizedLogger logger = new SynthesizedLogger(
                getAppName(),
                Level.DEBUG,
                propCenter.reportListenerAddr,
                propCenter.reportListenerPort);

        String checkpointDir = propCenter.checkpointRootDir + "/" + getAppName();
        logger.info("Using " + checkpointDir + " as checkpoint directory.");
        streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, () -> {
            JavaStreamingContext context = getStreamContext();
            try {
                if (propCenter.sparkMaster.contains("local")) {
                    File dir = new File(checkpointDir);
                    dir.delete();
                    dir.mkdirs();
                } else {
                    FileSystem fs = FileSystem.get(new Configuration());
                    Path dir = new Path(checkpointDir);
                    fs.delete(dir, true);
                    fs.mkdirs(dir);
                }
                context.checkpoint(checkpointDir);
            } catch (IllegalArgumentException | IOException e) {
                e.printStackTrace();
            }
            return context;
        }, new Configuration(), true);
    }

    /**
     * Start the application.
     */
    public void start() {
        streamingContext.start();
    }

    /**
     * Stop the application.
     */
    public void stop() {
        streamingContext.stop();
    }

    /**
     * Await termination of the application.
     *
     * @throws InterruptedException
     */
    public void awaitTermination() throws InterruptedException {
        streamingContext.awaitTermination();
    }

    @Override
    protected void finalize() throws Throwable {
        if (streamingContext != null) {
            streamingContext.close();
        }
        super.finalize();
    }
}
