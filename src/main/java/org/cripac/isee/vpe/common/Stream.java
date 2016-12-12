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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;
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
     * @param jssc A Spark Streaming context.
     */
    public abstract void addToContext(JavaStreamingContext jssc);

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
                           @Nonnull Map<String, Object> kafkaParams,
                           int procTime) {
        kafkaParams.put("enable.auto.commit", false);
        JavaInputDStream<ConsumerRecord<String, byte[]>> inputStream = KafkaUtils
                .createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(
                                topics,
                                kafkaParams));
        // TODO(Ken Yu): Check if this offset commit can help recovering
        // Kafka offset when checkpoint is deleted.
        inputStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            Thread.sleep(procTime);
            ((CanCommitOffsets) inputStream.inputDStream()).commitAsync(offsetRanges);
        });
        return inputStream.mapToPair(rec -> new Tuple2(rec.key(), rec.value()));
    }
}
