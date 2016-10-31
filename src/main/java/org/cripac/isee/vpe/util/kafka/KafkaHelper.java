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

package org.cripac.isee.vpe.util.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.cripac.isee.vpe.common.Topic;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by ken.yu on 16-10-27.
 */
public class KafkaHelper {

    public static <K, V> void sendWithLog(@Nonnull Topic topic,
                                          @Nonnull K key,
                                          @Nonnull V data,
                                          @Nonnull KafkaProducer<K, V> producer,
                                          @Nullable Logger logger)
            throws ExecutionException, InterruptedException {
        if (logger == null) {
            logger = new ConsoleLogger();
        }
        logger.debug("Sending to Kafka" + " <" + topic + ">\t" + key);
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
                topic.NAME,
                key,
                data));
        RecordMetadata recMeta = future.get();
        logger.debug("Sent to Kafka"
                + " <" + recMeta.topic() + "-"
                + recMeta.partition() + "-" + recMeta.offset() + ">\t"
                + key);
    }
}
