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
package org.cripac.isee.vpe.util.kafka

import java.util
import javax.annotation.{Nonnull, Nullable}

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster
import org.cripac.isee.vpe.common.Topic
import org.cripac.isee.vpe.util.logging.{ConsoleLogger, Logger}

import scala.collection.JavaConversions._
import scala.language.postfixOps

/**
  * The class KafkaHelper provides static basic methods for manipulating Kafka affairs.
  * It is written in Scala because we cannot create KafkaCluster in some versions of Java and Scala.
  * This class is also an example of how to insert Scala codes in the Java project.
  *
  * @author Ken Yu
  */
object KafkaHelper {
  /**
    * Send a message to Kafka with provided producer. Debug info is output to given logger.
    *
    * @param topic     The Kafka topic to send to.
    * @param key       Key of the message.
    * @param value     Value of the message.
    * @param producer  The Kafka producer to use to send the message.
    * @param extLogger The logger to output debug info.
    * @tparam K Type of the key.
    * @tparam V Type of the value.
    */
  def sendWithLog[K, V](@Nonnull topic: Topic,
                        @Nonnull key: K,
                        @Nonnull value: V,
                        @Nonnull producer: KafkaProducer[K, V],
                        @Nullable extLogger: Logger) {
    // Check if logger is provided. If not, create a console logger.
    val logger = if (extLogger == null) new ConsoleLogger() else extLogger
    // Send the message.
    logger debug ("Sending to Kafka <" + topic + ">\t" + key)
    val future = producer send new ProducerRecord[K, V](topic.NAME, key, value)
    // Retrieve sending report.
    try {
      val recMeta = future get;
      logger debug ("Sent to Kafka" + " <"
        + recMeta.topic + "-"
        + recMeta.partition + "-"
        + recMeta.offset + ">\t" + key)
    } catch {
      case e: InterruptedException =>
        logger error("Interrupted when retrieving Kafka sending result.", e)
    }
  }

  /**
    * Create a KakfaCluster with given Kafka parameters.
    *
    * @param kafkaParams Configuration parameters of the Kafka cluster.
    * @return A KafkaCluster instance.
    */
  def createKafkaCluster(@Nonnull kafkaParams: util.Map[String, Object]): KafkaCluster = {
    new KafkaCluster(kafkaParams.mapValues {
      case s: String => s
      case obj => obj.asInstanceOf[Integer].toString
    }.toMap)
  }

  /**
    * Get fromOffsets stored at a Kafka cluster.
    *
    * @param kafkaCluster The Kafka cluster.
    * @param topics       Topics the offsets belong to.
    * @return A map from each partition of each topic to the fromOffset.
    */
  @throws[SparkException]
  def getFromOffsets(@Nonnull kafkaCluster: KafkaCluster,
                     @Nonnull topics: util.Collection[String]
                    ): util.Map[TopicPartition, java.lang.Long] = {
    // Retrieve partition information of the topics from the Kafka cluster.
    val partitions = KafkaCluster.checkErrors(kafkaCluster.getPartitions(new util.HashSet[String](topics).toSet))

    // Retrieve offset metadata of the Kafka cluster.
    val earliestOffsets = KafkaCluster.checkErrors(kafkaCluster.getEarliestLeaderOffsets(partitions))
    val latestOffsets = KafkaCluster.checkErrors(kafkaCluster.getLatestLeaderOffsets(partitions))

    // Create a map to store corrected fromOffsets
    val fromOffsets = new util.HashMap[TopicPartition, java.lang.Long]
    // Retrieve consumer offsets.
    kafkaCluster getConsumerOffsets((kafkaCluster kafkaParams GROUP_ID_CONFIG).toString, partitions) match {
      // No offset (new group). Auto configure the offsets.
      case Left(_) =>
        val autoResetConfig = kafkaCluster kafkaParams AUTO_OFFSET_RESET_CONFIG
        val offsets = autoResetConfig match {
          case "largest" | "latest" => latestOffsets
          case "smallest" | "earliest" => earliestOffsets
        }
        offsets foreach (offset => fromOffsets put(
          new TopicPartition(offset._1.topic, offset._1.partition), offset._2.offset))
      // Store the offsets after checking the values.
      // If an offset is smaller than 0, change it to 0.
      case Right(consumerOffsets) =>
        consumerOffsets foreach (consumerOffset => {
          val topicAndPartition = consumerOffset._1
          val earliestOffset = earliestOffsets(topicAndPartition).offset
          val latestOffset = latestOffsets(topicAndPartition).offset
          fromOffsets put(new TopicPartition(topicAndPartition.topic, topicAndPartition.partition),
            if (consumerOffset._2 < earliestOffset) {
              println("Offset for " + topicAndPartition + " is out of date. Update to " + earliestOffset)
              earliestOffset
            } else if (consumerOffset._2 > latestOffset) {
              println("Offset for " + topicAndPartition + " is out of date. Update to " + latestOffset)
              latestOffset
            } else consumerOffset._2)
        })
    }

    // Return the fromOffsets
    fromOffsets
  }
}