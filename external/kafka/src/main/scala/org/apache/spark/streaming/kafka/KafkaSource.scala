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

package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.serializer._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

private[kafka]
case class KafkaSourceOffset(offsets: Map[TopicAndPartition, Long]) extends Offset {

  private lazy val offsetMap = offsets.toMap

  override def isEmpty: Boolean = offsets.isEmpty

  override def >(other: Offset): Boolean = {

    // For a topic+partition, this comparison returns true if
    // - exists in both `this` and `other`, and this offset > other offset
    // - does not exist in both `this` and `other

    other match {
      case KafkaSourceOffset(otherOffsets) =>
        otherOffsets.forall { case (otherTp, otherOffset) =>
          offsetMap.get(otherTp).map { _ >  otherOffset }.getOrElse { true }
        }
      case _ =>
        throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
    }
  }

  override def <(other: Offset): Boolean = {

    // For a topic+partition, this comparison returns true if
    // - exists in both `this` and `other`, and this offset < other offset
    // - does not exist in both `this` and `other

    other match {
      case KafkaSourceOffset(otherOffsets) =>
        otherOffsets.forall { case (otherTp, otherOffset) =>
          offsetMap.get(otherTp).map { _ < otherOffset }.getOrElse { true }
        }
      case _ =>
        throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
    }
  }

  def toMap: Map[TopicAndPartition, Long] = offsetMap

  override def toString(): String = offsets.toSeq.mkString("[", ", ", "]")
}

private[kafka] object KafkaSourceOffset {
  def fromOffset(offset: Offset): KafkaSourceOffset = {
    offset match {
      case o: KafkaSourceOffset => o
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to $getClass")
    }
  }
}


private[kafka] case class KafkaSource(
  topics: Set[String], params: Map[String, String]) extends Source {

  implicit private val encoder = ExpressionEncoder.tuple(
    ExpressionEncoder[Array[Byte]](), ExpressionEncoder[Array[Byte]]())

  private val kc = new KafkaCluster(params)
  private val topicAndPartitions = KafkaCluster.checkErrors(kc.getPartitions(topics))


  override def schema: StructType = encoder.schema

  /** Returns the maximum offset that can be retrieved from the source. */
  override def offset: Offset = {
    val partitionLeaders = KafkaCluster.checkErrors(kc.findLeaders(topicAndPartitions))
    val leadersAndOffsets = KafkaCluster.checkErrors(kc.getLatestLeaderOffsets(topicAndPartitions))
    println("Getting offsets " + leadersAndOffsets)
    KafkaSourceOffset(leadersAndOffsets.map {  x => (x._1, x._2.offset)})
  }

  /**
    * Returns the data between the `start` and `end` offsets.  This function must always return
    * the same set of data for any given pair of offsets.
    */
  override def getSlice(sqlContext: SQLContext, start: Option[Offset], end: Offset): RDD[InternalRow] = {
    println(s"Getting slice from $start to $end")
    val fromKafkaOffsets = start match {
      case Some(o) => KafkaSourceOffset.fromOffset(o).offsets.toSeq
      case None =>
        if (params.get("auto.offset.reset").map(_.toLowerCase) == Some("smallest")) {
          val topicPartitions = KafkaSourceOffset.fromOffset(end).offsets.keySet
          KafkaCluster.checkErrors(kc.getEarliestLeaderOffsets(topicPartitions)).mapValues(_.offset)
        } else Nil
    }
    val untilKafkaOffsets = KafkaSourceOffset.fromOffset(end)
    val offsetRanges = {
      val untilKafkaOffsetMap = untilKafkaOffsets.toMap
      fromKafkaOffsets.map { case (tp, fromOffset) =>
        untilKafkaOffsetMap.get(tp) match {
          case Some(untilOffset) => OffsetRange(tp, fromOffset, untilOffset)
          case None => OffsetRange(tp, fromOffset, fromOffset + 1)
        }
      }.toSeq
    }

    println("Creating RDD with offset ranges: " + offsetRanges)
    val kafkaRdd = if (offsetRanges.nonEmpty) {
      KafkaUtils.createRDD[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
        sqlContext.sparkContext, params, offsetRanges.toArray)
    } else {
      sqlContext.sparkContext.emptyRDD[(Array[Byte], Array[Byte])]
    }
    kafkaRdd.map(encoder.toRow).map(_.copy())
  }

  def toDS()(implicit sqlContext: SQLContext): Dataset[(Array[Byte], Array[Byte])] = {
    toDF.as[(Array[Byte], Array[Byte])]
  }

  def toDF()(implicit sqlContext: SQLContext): DataFrame = {
    Source.toDF(this)
  }

  override def toString(): String = "KafkaSource"
}
