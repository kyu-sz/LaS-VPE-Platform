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

import java.util.Properties
import java.util.concurrent.{CancellationException, ExecutionException}
import java.{lang => jl, util => ju}
import javax.annotation.{Nonnull, Nullable}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.common.Topic
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.security.JaasUtils
import org.cripac.isee.vpe.ctrl.TaskData
import org.cripac.isee.vpe.util.SerializationHelper
import org.cripac.isee.vpe.util.logging.{ConsoleLogger, Logger}

import scala.language.postfixOps

/**
  * The class KafkaHelper provides static basic methods for manipulating Kafka affairs.
  * It is written in Scala because some Scala objects and classes are not accessible in Java.
  * This class is also an example of how to insert Scala codes in the Java project.
  *
  * @author Ken Yu
  */
object KafkaHelper {
  /**
    * Send a message to Kafka with provided producer. Debug info is output to given logger.
    *
    * @param topic     the Kafka topic to send to.
    * @param key       key of the message.
    * @param value     value of the message.
    * @param producer  the Kafka producer to use to send the message.
    * @param extLogger the logger to output debug info.
    * @tparam K type of the key.
    * @tparam V type of the value.
    */
  def sendWithLog[K, V](
                         @Nonnull topic: String,
                         @Nonnull key: K,
                         @Nonnull value: V,
                         @Nonnull producer: KafkaProducer[K, V],
                         @Nullable extLogger: Logger
                       ): Unit = {
    // Check if logger is provided. If not, create a console logger.
    val logger = if (extLogger == null) new ConsoleLogger() else extLogger
    // Send the message.
    logger debug ("Sending to Kafka <" + topic + ">\t" + key)
    // Retrieve sending report.
    try {
      val future = producer send new ProducerRecord[K, V](topic, key, value)
      val recMeta = future get;
      logger debug ("Sent to Kafka" + " <"
        + recMeta.topic + "-"
        + recMeta.partition + "-"
        + recMeta.offset + ">\t" + key)
    } catch {
      case ie: InterruptedException =>
        logger error("Interrupted when retrieving Kafka sending result.", ie)
      case e@(_: ExecutionException | _: CancellationException) =>
        throw if (e.getCause != null) e.getCause else e
    }
  }

  /**
    * Send a TaskData to Kafka with provided producer. Debug info is output to given logger.
    *
    * @param key       key of the Kafka message.
    * @param taskData  the TaskData object to send.
    * @param producer  Kafka producer used to send the message.
    * @param extLogger logger for outputting debug info.
    * @tparam K type of key.
    */
  def sendWithLog[K](
                      @Nonnull key: K,
                      @Nonnull taskData: TaskData,
                      @Nonnull producer: KafkaProducer[K, Array[Byte]],
                      @Nullable extLogger: Logger
                    ): Unit = {
    sendWithLog(taskData.outputType.name(),
      key,
      SerializationHelper.serialize(taskData),
      producer,
      extLogger)
  }

  def createZkUtils(
                     zkServers: String,
                     sessionTimeout: Int,
                     connectionTimeout: Int
                   ): ZkUtils = {
    val (zkClient, zkConn) = ZkUtils.createZkClientAndConnection(zkServers, sessionTimeout, connectionTimeout)
    new ZkUtils(zkClient, zkConn, JaasUtils.isZkSecurityEnabled)
  }

  def createTopicIfNotExists(
                              zkServers: String,
                              sessionTimeout: Int,
                              connectionTimeout: Int,
                              topic: String,
                              partitions: Int,
                              replicas: Int
                            ): Unit = {
    createTopicIfNotExists(createZkUtils(zkServers, sessionTimeout, connectionTimeout),
      topic, partitions, replicas)
  }

  def createTopicIfNotExists(
                              zkUtils: ZkUtils,
                              topic: String,
                              partitions: Int,
                              replicas: Int
                            ): Unit = {
    createTopic(zkUtils, topic, partitions, replicas, ifNotExists = true)
  }

  def createTopic(
                   zkServers: String,
                   sessionTimeout: Int,
                   connectionTimeout: Int,
                   topic: String,
                   partitions: Int,
                   replicas: Int,
                   ifNotExists: Boolean
                 ): Unit = {
    createTopic(createZkUtils(zkServers, sessionTimeout, connectionTimeout),
      topic, partitions, replicas, ifNotExists)
  }

  def createTopic(
                   zkUtils: ZkUtils,
                   topic: String,
                   partitions: Int,
                   replicas: Int,
                   ifNotExists: Boolean
                 ) {
    val configs = new Properties
    if (Topic.hasCollisionChars(topic))
      println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.")
    try {
      val rackAwareMode = RackAwareMode.Enforced
      AdminUtils.createTopic(zkUtils, topic, partitions, replicas, configs, rackAwareMode)
      println("Created topic \"%s\".".format(topic))
    } catch {
      case e: TopicExistsException => if (!ifNotExists) throw e
    }
  }
}