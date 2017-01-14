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
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import scala.Tuple2;
import scala.collection.JavaConversions;

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

    protected final Singleton<Logger> loggerSingleton;

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

    protected final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

    private static class StringByteArrayRecord implements Serializable {
        String key;
        byte[] value;

        StringByteArrayRecord(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Utility function for all applications to receive messages with byte
     * array values from Kafka with direct stream.
     *
     * @param jssc        The streaming context of the applications.
     * @param topics      Topics from which the direct stream reads.
     * @param kafkaCluster Kafka cluster created from kafkaParams
     *                     (please use {@link KafkaHelper#createKafkaCluster(Map)}).
     * @return A Kafka non-receiver input stream.
     */
    protected JavaPairDStream<String, byte[]>
    buildBytesDirectStream(@Nonnull JavaStreamingContext jssc,
                           @Nonnull Collection<String> topics,
                           @Nonnull KafkaCluster kafkaCluster) {
        // Retrieve and correct offsets from Kafka cluster.
        final Map<TopicAndPartition, Long> consumerOffsetsLong = KafkaHelper.getFromOffsets(kafkaCluster, topics);

        // Create a direct stream from the retrieved offsets.
        final JavaPairDStream<String, byte[]> stream = KafkaUtils.createDirectStream(
                jssc,
                String.class, byte[].class,
                StringDecoder.class, DefaultDecoder.class,
                StringByteArrayRecord.class,
                JavaConversions.mapAsJavaMap(kafkaCluster.kafkaParams()),
                consumerOffsetsLong,
                (Function<MessageAndMetadata<String, byte[]>, StringByteArrayRecord>) messageAndMetadata ->
                        new StringByteArrayRecord(messageAndMetadata.key(), messageAndMetadata.message()))
                // Repartition the records.
                .repartition(jssc.sparkContext().defaultParallelism())
                // Store offsets.
                .transform((Function<JavaRDD<StringByteArrayRecord>, JavaRDD<StringByteArrayRecord>>) rdd -> {
                    final Logger logger = loggerSingleton.getInst();
                    boolean hasNewMessages = false;
                    for (OffsetRange o : offsetRanges.get()) {
                        if (o.untilOffset() > o.fromOffset()) {
                            hasNewMessages = true;
                            logger.debug("Received {topic=" + o.topic()
                                    + ", partition=" + o.partition()
                                    + ", fromOffset=" + o.fromOffset()
                                    + ", untilOffset=" + o.untilOffset() + "}");
                        }
                    }
                    if (!hasNewMessages) {
                        logger.debug("No new messages!");
                    }
                    return rdd;
                })
                // Transform to usual record type.
                .mapToPair((PairFunction<StringByteArrayRecord, String, byte[]>) consumerRecord ->
                        new Tuple2<>(consumerRecord.key, consumerRecord.value));
        return stream;
    }
}
