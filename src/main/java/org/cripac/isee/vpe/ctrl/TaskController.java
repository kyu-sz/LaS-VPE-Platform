/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-15.
 */
package org.cripac.isee.vpe.ctrl;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.cripac.isee.vpe.util.logging.Logger;

import java.util.*;
import java.util.stream.Collectors;

import static org.cripac.isee.vpe.common.DataType.TERM_SIG;

/**
 * The TaskController retrieves the latest 100 termination signals from Kafka and store in a pool.
 * A terminal signal is a task UUID sent to the TERM_SIG topic.
 * TODO: Provide interface for selecting tasks to terminal, so that users do not need to know the exact task UUIDs.
 */
public class TaskController extends Thread {

    public final Set<UUID> termSigPool = new HashSet<>();
    private final LinkedList<UUID> termSigQueue = new LinkedList<>();
    private final KafkaConsumer<String, byte[]> consumer;
    private final Logger logger;

    public TaskController(SystemPropertyCenter propCenter, Logger logger) {
        logger.debug("Constructing TaskController.");
        Properties consumerProp = propCenter.getKafkaConsumerProp(UUID.randomUUID().toString(), false);
        consumer = new KafkaConsumer<>(consumerProp);
        this.logger = logger;
    }

    @Override
    public void run() {
        logger.debug("TaskController preparing offsets.");

        final List<TopicPartition> topicPartitions = consumer.partitionsFor(TERM_SIG.name()).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
        logger.debug("TaskController retrieved partitions.");

        final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        logger.debug("TaskController retrieved beginning offsets.");
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        logger.debug("TaskController retrieved end offsets.");
        consumer.assign(topicPartitions);
        endOffsets.forEach((tp, offset) -> consumer.seek(tp, Math.max(offset - 100, beginningOffsets.get(tp))));

        logger.debug("Starting TaskController.");

        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                ConsumerRecords<String, byte[]> sigs = consumer.poll(1000);
                sigs.forEach(rec -> {
                    UUID taskID = UUID.fromString(rec.key());
                    termSigPool.add(taskID);
                    termSigQueue.add(taskID);
                    logger.info("Received term sig for task " + taskID);
                });
                while (termSigQueue.size() > 100) {
                    termSigPool.remove(termSigQueue.pollFirst());
                }
                consumer.commitSync();
            } catch (Exception e) {
                logger.error("During processing termination signals", e);
            }
        }
    }
}
