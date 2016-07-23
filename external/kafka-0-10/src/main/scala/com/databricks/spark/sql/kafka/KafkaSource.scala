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

package com.databricks.spark.sql.kafka


import java.{util => ju}

import scala.collection.JavaConverters._

import com.databricks.spark.sql.kafka.KafkaSource._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType


/** An [[Offset]] for the [[KafkaSource]]. */
private[kafka]
case class KafkaSourceOffset(partitionToOffsets: Map[TopicPartition, Long]) extends Offset {
  /**
   * Returns a negative integer, zero, or a positive integer as this object is less than, equal to,
   * or greater than the specified object.
   */
  override def compareTo(other: Offset): Int = other match {
    case KafkaSourceOffset(otherOffsets) =>
      val allTopicAndPartitions = (this.partitionToOffsets.keySet ++ otherOffsets.keySet).toSeq

      val comparisons = allTopicAndPartitions.map { tp =>
        (this.partitionToOffsets.get(tp), otherOffsets.get(tp)) match {
          case (Some(a), Some(b)) =>
            if (a < b) {
              -1
            } else if (a > b) {
              1
            } else {
              0
            }
          case (None, _) => -1
          case (_, None) => 1
        }
      }
      val nonZeroSigns = comparisons.filter { _ != 0 }.toSet
      nonZeroSigns.size match {
        case 0 => 0 // if both empty or only 0s
        case 1 => nonZeroSigns.head // if there are only (0s and 1s) or (0s and -1s)
        case _ => // there are both 1s and -1s
          throw new IllegalArgumentException(
            s"Invalid comparison between non-linear histories: $this <=> $other")
      }

    case _ =>
      throw new IllegalArgumentException(s"Cannot compare $this <=> $other")
  }

  override def toString(): String = {
    partitionToOffsets.toSeq.sortBy(_._1.toString).mkString("[", ", ", "]")
  }
}

/** Companion object of the [[KafkaSourceOffset]] */
private[kafka] object KafkaSourceOffset {

  def getPartitionOffsets(offset: Offset): Map[TopicPartition, Long] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(offsetTuples: Iterable[(TopicPartition, Long)]): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.toMap)
  }
}



/** A [[Source]] that reads data from Kafka */
case class KafkaSource(
    sqlContext: SQLContext,
    consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]],
    executorKafkaParams: ju.Map[String, Object],
    sourceOptions: Map[String, String])
  extends Source with Logging {

  implicit private val encoder = ExpressionEncoder.tuple(
    ExpressionEncoder[Array[Byte]](), ExpressionEncoder[Array[Byte]]())

  @transient private val consumer = consumerStrategy.createConsumer()
  @transient private val sc = sqlContext.sparkContext
  @transient private val initialPartitionOffsets = fetchPartitionOffsets(seekToLatest = false)
  logInfo(s"Initial offsets: " + initialPartitionOffsets)

  override def schema: StructType = encoder.schema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    val offset = KafkaSourceOffset(fetchPartitionOffsets(seekToLatest = true))
    logInfo(s"GetOffset: $offset")
    Some(offset)
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
   * the batch should begin with the first available record. This method must always return the
   * same data for a particular `start` and `end` pair.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logDebug(s"GetBatch called with start = $start, end = $end")
    val endPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    val startPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }
    val sortedTopicPartitions = endPartitionOffsets.keySet.toSeq.sorted
    val sortedExecutors = SparkInternalUtils.getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.size
    logInfo("Sorted executors: " + sortedExecutors.mkString(", "))
    val offsetRanges = sortedTopicPartitions.flatMap { tp =>
      startPartitionOffsets.get(tp).map { startOffset =>
        val endOffset = endPartitionOffsets(tp)
        val preferredLoc = if (numExecutors > 0) {
          Some(sortedExecutors(positiveMod(tp.hashCode, numExecutors)))
        } else None
        KafkaSourceRDD.OffsetRange(tp, startOffset, endOffset, preferredLoc)
      }
    }.toArray

    val rdd = new KafkaSourceRDD[Array[Byte], Array[Byte]](
      sc, executorKafkaParams, offsetRanges, sourceOptions)
      .map { r => (r.key, r.value) }

    logInfo("GetBatch: " + offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))
    sqlContext.createDataset(rdd).toDF("key", "value")
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    consumer.close()
  }

  override def toString(): String = s"KafkaSource[$consumerStrategy]"

  private def fetchPartitionOffsets(seekToLatest: Boolean): Map[TopicPartition, Long] = {
    synchronized {
      logTrace("\tPolling")
      consumer.poll(0)
      val partitions = consumer.assignment()
      consumer.pause(partitions)
      logDebug(s"\tPartitioned assigned to consumer: $partitions")
      if (seekToLatest) {
        consumer.seekToEnd(partitions)
        logDebug("\tSeeked to the end")
      }
      logTrace("Getting positions")
      val partitionToOffsets = partitions.asScala.map(p => p -> consumer.position(p))
      logDebug(s"Got positions $partitionToOffsets")
      partitionToOffsets.toMap
    }
  }

  private def positiveMod(a: Long, b: Int): Int = ((a % b).toInt + b) % b
}

object KafkaSource {

  sealed trait ConsumerStrategy[K, V] {
    def createConsumer(): Consumer[K, V]
  }

  case class SubscribeStrategy[K, V](topics: Seq[String], kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy[K, V] {
    override def createConsumer(): Consumer[K, V] = {
      val consumer = new KafkaConsumer[K, V](kafkaParams)
      consumer.subscribe(topics.asJava)
      consumer.poll(0)
      consumer
    }

    override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
  }

  case class SubscribePatternStrategy[K, V](
    topicPattern: String, kafkaParams: ju.Map[String, Object])
    extends ConsumerStrategy[K, V] {
    override def createConsumer(): Consumer[K, V] = {
      val consumer = new KafkaConsumer[K, V](kafkaParams)
      consumer.subscribe(
        ju.regex.Pattern.compile(topicPattern),
        new NoOpConsumerRebalanceListener())
      consumer.poll(0)
      consumer
    }

    override def toString: String = s"SubscribePattern[$topicPattern]"
  }
}




class KafkaSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {
  private val structType = new StructType().add("key", "binary").add("value", "binary")

  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, Object](kafkaParams.asJava)
    def set(key: String, value: Object): this.type = {
      logInfo(s"$module: Setting $key to $value, earlier value: ${kafkaParams.get(key)}")
      map.put(key, value)
      this
    }

    def setIfUnset(key: String, value: Object): ConfigUpdater = {
      if (!map.containsKey(key)) {
        logInfo(s"$module: Setting $key to $value")
        map.put(key, value)
      }
      this
    }

    def build(): ju.Map[String, Object] = {
      map
    }
  }

    /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    ("kafka", structType)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v)}

    val strategyOptionNames = Set("subscribe", "subscribepattern")
    val specifiedStrategies =
      caseInsensitiveParams.filter { case(k, _) => strategyOptionNames.contains(k) }.toSeq
    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for Kafka source: "
          + strategyOptionNames.mkString(", ") + ". See docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for Kafka source: "
          + strategyOptionNames.mkString(", ") + ". See docs for more details.")
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}")) {
      throw new IllegalArgumentException(
        "Option 'kafka.bootstrap.servers' must be specified for configuring Kafka consumer")
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        "Option 'kafka.group.id' must be specified for configuring Kafka consumer")
    }

    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val deserClassName = classOf[ByteArrayDeserializer].getName


    val kafkaParamsForStrategy =
      ConfigUpdater("source", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .build()

    val kafkaParamsForExecutors =
      ConfigUpdater("source", specifiedKafkaParams)
        .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
        .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)

        // So that consumers in executors never throw NoOffsetForPartitionException
        .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

        // So that consumers in executors do not mess with user-specified group id
        .set(ConsumerConfig.GROUP_ID_CONFIG,
          "spark-executor-" + specifiedKafkaParams(ConsumerConfig.GROUP_ID_CONFIG))

        // So that consumers in executors no keep committing offsets unnecessaribly
        .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        // If buffer config is not set, it to reasonable value to work around
        // buffer issues (see KAFKA-3135)
        .setIfUnset(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
        .build()

    val strategy = specifiedStrategies.head match {
      case ("subscribe", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            "No topics to subscribe to as specified value for option " +
              s"'subscribe' is '$value'")
        }
        SubscribeStrategy[Array[Byte], Array[Byte]](
          topics, kafkaParamsForStrategy)

      case ("subscribepattern", value) =>
        val pattern = caseInsensitiveParams("subscribepattern").trim()
        if (pattern.isEmpty) {
          throw new IllegalArgumentException(
            "Pattern to subscribe is empty as specified value for option " +
              s"'subscribePattern' is '$value'"
          )
        }
        SubscribePatternStrategy[Array[Byte], Array[Byte]](
          pattern, kafkaParamsForStrategy)
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown strategy")
    }

    new KafkaSource(sqlContext, strategy, kafkaParamsForExecutors, parameters)
  }

  override def shortName(): String = "kafka"
}
