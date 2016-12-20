package org.cripac.isee.vpe.util.kafka

import java.util
import java.util.UUID
import java.util.concurrent.ExecutionException
import javax.annotation.{Nonnull, Nullable}

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.NetworkException
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import org.cripac.isee.vpe.common.Topic
import org.cripac.isee.vpe.util.logging.{ConsoleLogger, Logger}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

object KafkaHelper {

  @throws[ExecutionException]
  @throws[InterruptedException]
  def sendWithLog[K, V](@Nonnull topic: Topic,
                        @Nonnull key: K,
                        @Nonnull data: V,
                        @Nonnull producer: KafkaProducer[K, V],
                        @Nullable extLogger: Logger) {
    val logger = if (extLogger == null) new ConsoleLogger() else extLogger;
    logger debug ("Sending to Kafka <" + topic + ">\t" + key)
    val future = producer send
      new ProducerRecord[K, V](topic.NAME, key, data)
    val recMeta = future get;
    logger debug ("Sent to Kafka" + " <"
      + recMeta.topic + "-"
      + recMeta.partition + "-"
      + recMeta.offset + ">\t" + key)
  }

  /**
    * Submit currently consumed offsets to Kafka brokers.
    *
    * @param offsetRanges An array of OffsetRange.
    * @param kafkaParams  Parameters for connecting to the Kafka cluster.
    */
  def submitOffset(@Nonnull offsetRanges: Array[OffsetRange],
                   @Nonnull kafkaParams: util.Map[String, String]): Unit = {
    val kafkaCluster = new KafkaCluster(JavaConversions.mapAsScalaMap(kafkaParams).toMap)
    for (o <- offsetRanges) {
      val topicAndPartition = new TopicAndPartition(o.topic, o.partition)
      val topicAndPartitionOffsetMap = collection.mutable.Map[TopicAndPartition, Long]()
      topicAndPartitionOffsetMap += topicAndPartition -> o.untilOffset
      kafkaCluster setConsumerOffsets(kafkaParams get (ConsumerConfig GROUP_ID_CONFIG), topicAndPartitionOffsetMap.toMap)
    }
  }

  /**
    * Get fromOffsets stored at Kafka brokers.
    *
    * @param topics      Topics the offsets belong to.
    * @param kafkaParams Parameters for connection to the Kafka brokers.
    * @return A map from each partition of each topic to the fromOffset.
    */
  def getFromOffsets(@Nonnull topics: util.Collection[String],
                     @Nonnull kafkaParams: util.Map[String, String]) = {
    val kafkaCluster = new KafkaCluster(JavaConversions.mapAsScalaMap(kafkaParams).toMap)
    val scalaTopics = JavaConversions asScalaSet (new util.HashSet[String](topics)) toSet
    val partitionOrErr = kafkaCluster getPartitions scalaTopics
    if (partitionOrErr.isLeft) throw new NetworkException("Cannot retrieve partitions from Kafka cluster!")
    val topicAndPartitionSet = partitionOrErr.right.get
    val consumerOffsetsLong = new util.HashMap[TopicAndPartition, java.lang.Long]
    val offsetsOrErr = kafkaCluster getConsumerOffsets(
      kafkaParams get ConsumerConfig.GROUP_ID_CONFIG,
      topicAndPartitionSet);
    if (offsetsOrErr isLeft)
    // No offset (new group).
      topicAndPartitionSet foreach (consumerOffsetsLong put(_, 0L))
    else {
      val offsets = offsetsOrErr.right.get
      offsets foreach (x => consumerOffsetsLong put(x._1,
        if (x._2 < 0L) long2Long(0L) else long2Long(x._2)))
    }
    consumerOffsetsLong
  }

  private val TIMEOUT = 100000
  private val BUFFER_SIZE = 64 * 1024

  /**
    * Get the last offsets of the topics in given Kafka brokers.
    *
    * @param brokerList A list of Kafka brokers.
    * @param topics     Topics to check.
    * @param groupId    Group ID of the Kafka consumer.
    * @return Offset at each partition of each topic.
    */
  def getUntilOffsets(@Nonnull brokerList: String,
                      @Nonnull topics: util.Collection[String],
                      @Nonnull groupId: String) = {
    // Create result buffer.
    var topicAndPartitionOffsetMap = new util.HashMap[TopicAndPartition, java.lang.Long]
    // Find leaders in the brokers for the given topics.
    val topicAndPartitionBrokerMap = findLeaders(brokerList, topics)

    for ((topicAndPartition, leaderBroker) <- topicAndPartitionBrokerMap) {
      // Create a simple consumer to probe into the topic.
      val simpleConsumer = new SimpleConsumer(leaderBroker.host, leaderBroker.port, TIMEOUT, BUFFER_SIZE, groupId)
      // Retrieve current reading offset.
      val readOffset = getTopicAndPartitionLastOffset(simpleConsumer, topicAndPartition, groupId)
      // Store the offset as the latest offset.
      topicAndPartitionOffsetMap += topicAndPartition -> readOffset
    }
    topicAndPartitionOffsetMap
  }

  /**
    * Retrieve all TopicAndPartition
    *
    * @param brokerList List of Kafka brokers.
    * @param topics     Topics of the request.
    * @return topicAndPartitions
    */
  private def findLeaders(@Nonnull brokerList: String, @Nonnull topics: util.Collection[String]) = {
    // Get the brokers' addresses
    val brokerAddresses = brokerList.split(",").map(_.split(":")(0))
    // Get the brokers' port map
    val brokerPortMap = getPortFromBrokerList(brokerList)
    // Create result buffer.
    val topicAndPartitionBrokerMap = collection.mutable.Map[TopicAndPartition, Broker]()
    // For each broker.
    for (brokerAddr <- brokerAddresses) {
      var consumer: SimpleConsumer = null;
      try {
        // Create new instance of simple Kafka consumer.
        consumer = new SimpleConsumer(brokerAddr, brokerPortMap(brokerAddr),
          TIMEOUT, BUFFER_SIZE, "leaderLookup" + UUID.randomUUID)
        // Send a metadata request to the broker.
        val list: Seq[String] = new util.ArrayList[String](topics)
        val req = new TopicMetadataRequest(list, 0)
        // Retrieve topic metadata.
        val resp = consumer.send(req)
        val metaData = resp.topicsMetadata
        // Extract topic and partition pairs from the metadata and put into the result buffer.
        for (item <- metaData)
          for (part <- item.partitionsMetadata) {
            val topicAndPartition = new TopicAndPartition(item.topic, part.partitionId)
            topicAndPartitionBrokerMap += topicAndPartition -> part.leader.get
          }
      }
      catch {
        case e: Exception => {
          e.printStackTrace()
        }
      } finally if (consumer != null) consumer.close()
    }
    topicAndPartitionBrokerMap
  }

  /**
    * Get the last offset from a consumer.
    *
    * @param consumer          A Kafka consumer which has established link.
    * @param topicAndPartition A specific partition of a topic.
    * @param groupID           Group ID of the consumer.
    * @return The latest offset the consumer is reading from the partition of the topic. Returns 0 on error.
    */
  private def getTopicAndPartitionLastOffset(@Nonnull consumer: SimpleConsumer,
                                             @Nonnull topicAndPartition: TopicAndPartition,
                                             @Nonnull groupID: String): Long = {
    // Create a offset request information buffer.
    val reqInfo = collection.mutable.Map[TopicAndPartition, PartitionOffsetRequestInfo]()
    // Fill in the offset request information.
    reqInfo += topicAndPartition -> new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime, 1)
    // Create a offset request with the prepared information.
    val request = new OffsetRequest(reqInfo.toMap, kafka.api.OffsetRequest.CurrentVersion, 0, groupID, 0)
    // Send the offset request and retrieve response.
    val response = consumer.getOffsetsBefore(request)
    // Check error in response.
    if (response.hasError) {
      System.err.println("Error fetching data Offset Data the Broker. Reason: "
        + response.partitionErrorAndOffsets(topicAndPartition).error)
      return 0L
    }
    // Get offsets from the offset response.
    val offsets = response.offsetsGroupedByTopic(topicAndPartition.topic)(topicAndPartition).offsets
    offsets(0)
  }

  /**
    * Analyze correspondence of Kafka broker addresses and ports from a broker list.
    *
    * @param brokerlist A list of brokers, containing addresses and ports, split by comma.
    * @return A map from broker addresses to ports.
    */
  private def getPortFromBrokerList(@Nonnull brokerlist: String) = {
    val map = collection.mutable.Map[String, Int]()
    brokerlist.split(",").foreach(brokerItem => {
      var itemPart = brokerItem.split(":")
      map += itemPart(0) -> itemPart(1).toInt
    })
    map
  }
}