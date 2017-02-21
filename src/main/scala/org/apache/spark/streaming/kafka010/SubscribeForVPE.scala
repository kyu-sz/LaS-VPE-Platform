/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka010

import java.{lang => jl, util => ju}

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

/**
  * [[SubscribeForVPE()]] is derived from [[Subscribe]]. The only difference is that [[SubscribeForVPE()]]
  * uses [[KafkaConsumerForVPE]] while [[Subscribe]] uses [[KafkaConsumer]].
  *
  * Subscribe to a collection of topics.
  *
  * @param topics      collection of topics to subscribe
  * @param kafkaParams Kafka
  *                    <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
  *                    configuration parameters</a> to be used on driver. The same params will be used on executors,
  *                    with minor automatic modifications applied.
  *                    Requires "bootstrap.servers" to be set
  *                    with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsets     : offsets to begin at on initial startup.  If no offset is given for a
  *                    TopicPartition, the committed offset (if applicable) or kafka param
  * auto.offset.reset will be used.
  */
private case class SubscribeForVPE[K, V](
                                          topics: ju.Collection[jl.String],
                                          kafkaParams: ju.Map[String, Object],
                                          offsets: ju.Map[TopicPartition, jl.Long]
                                        ) extends ConsumerStrategy[K, V] with Logging {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumerForVPE[K, V](kafkaParams)
    consumer.subscribe(topics)
    val toSeek = if (currentOffsets.isEmpty) {
      offsets
    } else {
      currentOffsets
    }
    if (!toSeek.isEmpty) {
      // work around KAFKA-3370 when reset is none
      // poll will throw if no position, i.e. auto offset reset none and no explicit position
      // but cant seek to a position before poll, because poll is what gets subscription partitions
      // So, poll, suppress the first exception, then seek
      val aor = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      val shouldSuppress = aor != null && aor.asInstanceOf[String].toUpperCase == "NONE"
      try {
        consumer.poll(0)
      } catch {
        case _: NoOffsetForPartitionException if shouldSuppress =>
          logWarning("Catching NoOffsetForPartitionException since " +
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + " is none.  See KAFKA-3370")
      }
      toSeek.asScala.foreach { case (topicPartition, offset) =>
        consumer.seek(topicPartition, offset)
      }
      // we've called poll, we must pause or next poll may consume messages and set position
      consumer.pause(consumer.assignment())
    }

    consumer
  }
}