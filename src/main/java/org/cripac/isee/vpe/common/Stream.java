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

import kafka.common.TopicAndPartition;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one INPUT_TYPE of output.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {

    /**
     * The Info class is designed to force the output data INPUT_TYPE to
     * be assigned to a stream, so that INPUT_TYPE matching checking can
     * be conducted.
     */
    public static class Info implements Serializable {
        /**
         * Name of the stream.
         */
        public final String NAME;

        /**
         * Type of output.
         */
        public final DataType OUTPUT_TYPE;

        /**
         * Construct a stream with NAME specified.
         *
         * @param name Name of the stream.
         */
        public Info(String name, DataType outputType) {
            this.NAME = name;
            this.OUTPUT_TYPE = outputType;
        }

        @Override
        public int hashCode() {
            return NAME.hashCode();
        }

        @Override
        public String toString() {
            return "[" + OUTPUT_TYPE + "]" + NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Info) {
                assert OUTPUT_TYPE == ((Info) o).OUTPUT_TYPE;
                return NAME.equals(((Info) o).NAME);
            } else {
                return super.equals(o);
            }
        }
    }

    /**
     * Add the stream to a Spark Streaming context.
     *
     * @param jsc A Spark Streaming context.
     */
    public abstract void addToContext(JavaStreamingContext jsc);

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
    protected JavaPairDStream<String, byte[]>
    buildBytesParRecvStream(@Nonnull JavaStreamingContext streamingContext,
                            int numRecvStreams,
                            @Nonnull Map<String, String> kafkaParams,
                            @Nonnull Map<String, Integer> numPartitionsPerTopic) {
        // Read bytes in parallel from Kafka.
        List<JavaPairDStream<String, byte[]>> parStreams = new ArrayList<>(numRecvStreams);
        for (int i = 0; i < numRecvStreams; i++) {
            parStreams.add(KafkaUtils.createStream(
                    streamingContext,
                    String.class, byte[].class,
                    StringDecoder.class, DefaultDecoder.class,
                    kafkaParams,
                    numPartitionsPerTopic,
                    StorageLevel.MEMORY_AND_DISK_SER()));
        }
        // Union the parallel bytes streams.
        return streamingContext.union(parStreams.get(0), parStreams.subList(1, parStreams.size()));
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param streamingContext      The streaming context of the applications.
     * @param kafkaParams           Parameters for reading from Kafka.
     * @param numPartitionsPerTopic A map specifying topics to read from, each assigned number of
     *                              partitions for the topic.
     * @return A Kafka non-receiver input stream.
     */
    protected JavaPairDStream<String, byte[]>
    buildBytesDirectStream(@Nonnull JavaStreamingContext streamingContext,
                           @Nonnull Map<String, String> kafkaParams,
                           @Nonnull Map<String, Integer> numPartitionsPerTopic) {
        return KafkaUtils
                // TODO(Ken Yu): Fetch offset from Zookeeper and restart from that.
                .createDirectStream(
                        streamingContext,
                        String.class, byte[].class,
                        StringDecoder.class, DefaultDecoder.class,
                        kafkaParams,
                        numPartitionsPerTopic.keySet())
                .transformToPair(rdd -> {
                    // TODO(Ken Yu): Report offset to Zookeeper.
                    rdd.context().setLocalProperty("spark.scheduler.pool", "vpe");
                    return rdd;
                });
    }
}
