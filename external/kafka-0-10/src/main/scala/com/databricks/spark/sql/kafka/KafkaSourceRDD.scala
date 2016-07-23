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

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.databricks.spark.sql.kafka.KafkaSourceRDD._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.CachedKafkaConsumer


private[kafka]
case class KafkaSourceRDDPartition(index: Int, offsetRange: OffsetRange) extends Partition

/**
 * A batch-oriented interface for consuming from Kafka.
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 * @param executorKafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
 * configuration parameters</a>. Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
private[kafka] class KafkaSourceRDD[K, V](
    sc: SparkContext,
    executorKafkaParams: ju.Map[String, Object],
    offsetRanges: Seq[OffsetRange],
    options: Map[String, String]) extends RDD[ConsumerRecord[K, V]](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new KafkaSourceRDDPartition(i, o) }.toArray
  }

  override def count(): Long = offsetRanges.map(_.size).sum

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[ConsumerRecord[K, V]] = {
    val nonEmptyPartitions =
      this.partitions.map(_.asInstanceOf[KafkaSourceRDDPartition]).filter(_.offsetRange.size > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return new Array[ConsumerRecord[K, V]](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.offsetRange.size)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[ConsumerRecord[K, V]]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[ConsumerRecord[K, V]]) =>
      it.take(parts(tc.partitionId)).toArray, parts.keys.toArray
    )
    res.foreach(buf ++= _)
    buf.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[ConsumerRecord[K, V]] = {
    val range = thePart.asInstanceOf[KafkaSourceRDDPartition].offsetRange
    assert(
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(s"Beginning offset ${range.fromOffset} is the same as ending offset " +
        s"skipping ${range.topic} ${range.partition}")
      Iterator.empty
    } else {
      new KafkaRDDIterator(range, executorKafkaParams, options, context)
    }
  }
}

private[kafka] object KafkaSourceRDD {

  case class OffsetRange(
      topicPartition: TopicPartition,
      fromOffset: Long,
      untilOffset: Long,
      preferredLoc: Option[String]) {
    def topic: String = topicPartition.topic

    def partition: Int = topicPartition.partition

    def size: Long = untilOffset - fromOffset
  }

  /**
   * An iterator that fetches messages directly from Kafka for the offsets in partition.
   * Uses a cached consumer where possible to take advantage of prefetching
   */
  private class KafkaRDDIterator[K, V](
      part: OffsetRange,
      executorKafkaParams: ju.Map[String, Object],
      options: Map[String, String],
      context: TaskContext) extends Iterator[ConsumerRecord[K, V]] with org.apache.spark
  .sql.Logging {

    logInfo(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val groupId = executorKafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

    private val pollTimeout =
      getLong(options, "consumer.pollMs", 512)
    private val cacheInitialCapacity =
      getInt(options, "consumer.cache.initialCapacity", 16)
    private val cacheMaxCapacity =
      getInt(options, "consumer.cache.maxCapacity", 64)
    private val cacheLoadFactor =
      getDouble(options, "consumer.cache.loadFactor", 0.75).toFloat

    CachedKafkaConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
    if (context.attemptNumber > 1) {
      // just in case the prior attempt failures were cache related
      CachedKafkaConsumer.remove(groupId, part.topic, part.partition)
    }
    val consumer =
      CachedKafkaConsumer.get[K, V](groupId, part.topic, part.partition, executorKafkaParams)

    var requestOffset = part.fromOffset

    override def hasNext(): Boolean = requestOffset < part.untilOffset

    override def next(): ConsumerRecord[K, V] = {
      assert(hasNext(), "Can't call next() once untilOffset has been reached")
      val r = consumer.get(requestOffset, pollTimeout)
      requestOffset += 1
      r
    }
  }

  def getInt(options: Map[String, String], name: String, defaultValue: Int): Int = {
    options.get(name).map { str =>
      Try(str.toInt).getOrElse {
        throw new IllegalArgumentException("Option '$name' must be a integer")
      }
    }.getOrElse(defaultValue)
  }

  def getDouble(options: Map[String, String], name: String, defaultValue: Double): Double = {
    options.get(name).map { str =>
      Try(str.toDouble).getOrElse {
        throw new IllegalArgumentException("Option '$name' must be a double")
      }
    }.getOrElse(defaultValue)
  }

  def getLong(options: Map[String, String], name: String, defaultValue: Long): Long = {
    options.get(name).map { str =>
      Try(str.toLong).getOrElse {
        throw new IllegalArgumentException("Option '$name' must be a long")
      }
    }.getOrElse(defaultValue)
  }
}
