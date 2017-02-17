package org.cripac.isee.vpe.util.kafka

import javax.annotation.{Nonnull, Nullable}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.cripac.isee.vpe.common.Topic
import org.cripac.isee.vpe.util.logging.{ConsoleLogger, Logger}

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
}