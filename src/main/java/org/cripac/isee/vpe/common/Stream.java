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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import scala.Tuple2;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one INPUT_TYPE of output.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {

    private static final long serialVersionUID = 7965952554107861881L;

    /**
     * The Info class is designed to force the output data INPUT_TYPE to
     * be assigned to a stream, so that INPUT_TYPE matching checking can
     * be conducted.
     */
    public static class Info implements Serializable {
        private static final long serialVersionUID = -2859100367977900846L;
        /**
         * Name of the stream.
         */
        public final String NAME;

        /**
         * Type of output.
         */
        public final DataTypes OUTPUT_TYPE;

        /**
         * Construct a stream with NAME specified.
         *
         * @param name Name of the stream.
         */
        public Info(String name, DataTypes outputType) {
            this.NAME = name;
            this.OUTPUT_TYPE = outputType;
        }

        @Override
        public int hashCode() {
            return NAME.hashCode();
        }

        @Override
        public String toString() {
            return "Topic(name: \'" + NAME + "\', output type: \'" + OUTPUT_TYPE + "\')";
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

    protected Singleton<Logger> loggerSingleton;

    /**
     * Require inputting a logger singleton for this class and all its subclasses.
     *
     * @param loggerSingleton A singleton of logger.
     */
    public Stream(@Nonnull Singleton<Logger> loggerSingleton) {
        this.loggerSingleton = loggerSingleton;
    }

    /**
     * Add the stream to a Spark Streaming context.
     *
     * @param jssc A Spark Streaming context.
     */
    public abstract void addToContext(JavaStreamingContext jssc);

    public static class ByteArrayRecord {
        private final String key;
        private final byte[] value;

        public ByteArrayRecord(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public byte[] value() {
            return value;
        }
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param jssc        The streaming context of the applications.
     * @param kafkaParams Parameters for reading from Kafka.
     * @param topics      Topics from which the direct stream reads.
     * @param procTime    Estimated time in milliseconds to be consumed in the
     *                    following process of each RDD from the input stream.
     *                    After this time, it is viewed that output has
     *                    finished, and offsets will be committed to Kafka.
     * @return A Kafka non-receiver input stream.
     */
    protected JavaPairDStream<String, byte[]>
    buildBytesDirectStream(@Nonnull JavaStreamingContext jssc,
                           @Nonnull Collection<String> topics,
                           @Nonnull Map<String, String> kafkaParams,
                           int procTime) {
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Retrieve fromOffsets stored at Kafka brokers.
        Map<TopicAndPartition, Long> fromOffsets = KafkaHelper.getFromOffsets(topics, kafkaParams);

        // Retrieve untilOffsets.
        Map<TopicAndPartition, Long> untilOffsets =
                KafkaHelper.getUntilOffsets(
                        kafkaParams.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                        topics,
                        kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG));

        // For each partition in each topic.
        fromOffsets.entrySet().forEach(fromOffset -> {
            // If fromOffset > untilOffset
            if (fromOffset.getValue() > untilOffsets.get(fromOffset.getKey())) {
                // Reset the fromOffset to zero.
                fromOffset.setValue(0L);
            }
        });

        // create direct stream
        JavaInputDStream<ByteArrayRecord> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                byte[].class,
                StringDecoder.class,
                DefaultDecoder.class,
                ByteArrayRecord.class,
                kafkaParams,
                fromOffsets,
                v1 -> new ByteArrayRecord(v1.key(), v1.message())
        );

        // Save the offsets of each partition of each topic.
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<ByteArrayRecord> javaDStream = message.transform(rdd-> {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
        });

        // Submit offsets to Kafka cluster.
        javaDStream.foreachRDD(v1 -> {
            if (!v1.isEmpty()) {
                KafkaHelper.submitOffset(offsetRanges.get(), kafkaParams);
            }
        });

        return message.mapToPair(record -> new Tuple2<>(record.key(), record.value()))
                .repartition(jssc.sparkContext().defaultParallelism());
    }
}
