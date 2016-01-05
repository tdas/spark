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
package org.apache.spark.streaming.kinesis

import scala.collection.JavaConverters._

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.{GetRecordsRequest, GetShardIteratorRequest}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}

private[kinesis] case class StreamAndShard(streamName: String, shardId: String)

private[kinesis] case class KinesisSourceOffset(seqNums: Map[StreamAndShard, String]) extends Offset {

  override def isEmpty: Boolean = false

  override def >(other: Offset): Boolean = {

    // For a shard, this comparison returns true if sequence number
    // - exists in both `this` and `other`, and this seq num > other seq num
    // - does not exist in both `this` and `other`

    other match {
      case KinesisSourceOffset(otherSeqNums) =>
        otherSeqNums.forall { case (otherShardId, otherSeqNum) =>
          seqNums.get(otherShardId).map { BigInt(_) > BigInt(otherSeqNum) }.getOrElse { true }
        }
      case _ =>
        throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
    }
  }

  override def <(other: Offset): Boolean = {

    // For a shard, this comparison returns true if sequence number
    // - exists in both `this` and `other`, and this seq num < other seq num
    // - does not exist in both `this` and `other`

    other match {
      case KinesisSourceOffset(otherSeqNums) =>
        otherSeqNums.forall { case (otherShardId, otherSeqNum) =>
          seqNums.get(otherShardId).map { BigInt(_) < BigInt(otherSeqNum) }.getOrElse { true }
        }
      case _ =>
        throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
    }
  }
}


private[kinesis] object KinesisSourceOffset {
  def fromOffset(offset: Offset): KinesisSourceOffset = {
    offset match {
      case o: KinesisSourceOffset => o
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to $getClass")
    }
  }
}



private[kinesis] case class KinesisSource(
    regionName: String,
    endpointUrl: String,
    streamNames: Set[String],
    initialPositionInStream: InitialPositionInStream,
    awsCredentialsOption: Option[SerializableAWSCredentials]) extends Source {

  implicit private val encoder = ExpressionEncoder[Array[Byte]]

  @transient val credentials = awsCredentialsOption.getOrElse {
    new DefaultAWSCredentialsProviderChain().getCredentials()
  }

  @transient private val client = new AmazonKinesisClient(credentials)
  client.setEndpoint(endpointUrl, "kinesis", regionName)

  @transient private val initialSeqNums = getInitialSeqNums()

  override def offset: Offset = {
    val endSequenceNumbers = streamNames.flatMap { streamName =>
      val desc = client.describeStream(streamName)
      desc.getStreamDescription.getShards.asScala.flatMap { shard =>
        val shardId = shard.getShardId
        println(s"\tshard: $shardId: ${shard.getSequenceNumberRange}")
        val seqNumOption = Option(shard.getSequenceNumberRange.getEndingSequenceNumber).orElse {
          getLatestSeqNum(streamName, shardId)
        }

        seqNumOption.map { s => (StreamAndShard(streamName, shardId), s) }
      }
    }.toMap
    println("Got offset: " + endSequenceNumbers)
    KinesisSourceOffset(endSequenceNumbers)
  }

  override def schema: StructType = encoder.schema

  override def getSlice(
      sqlContext: SQLContext,
      start: Option[Offset],
      end: Offset): RDD[InternalRow] = {
    val seqNumRanges = getSeqNumRanges(start, end)
    println("Seq num ranges: " + seqNumRanges.mkString(", "))
    val sparkContext = sqlContext.sparkContext
    val rdd = if (seqNumRanges.nonEmpty) {
      new KinesisRDD(sparkContext, regionName, endpointUrl, seqNumRanges, awsCredentialsOption)
    } else {
      sparkContext.emptyRDD[Array[Byte]]
    }
    rdd.map(encoder.toRow).map(_.copy())
  }

  def toDS()(implicit sqlContext: SQLContext): Dataset[Array[Byte]] = {
    toDF.as[Array[Byte]]
  }

  def toDF()(implicit sqlContext: SQLContext): DataFrame = {
    Source.toDF(this)
  }

  private def getSeqNumRanges(start: Option[Offset], end: Offset): Seq[SequenceNumberRange] = {
    val fromSeqNums = start match {
      case Some(o) => KinesisSourceOffset.fromOffset(o).seqNums
      case None => initialSeqNums
    }
    val toSeqNums = KinesisSourceOffset.fromOffset(end).seqNums
    val allStreamAndShards = (fromSeqNums.keySet ++ toSeqNums.keySet).toSeq
    allStreamAndShards.flatMap { case (ss) =>
      (fromSeqNums.get(ss), toSeqNums.get(ss)) match {

        case (Some(fromSeqNum), Some(toSeqNum)) =>
          Some(SequenceNumberRange(ss.streamName, ss.shardId, fromSeqNum, toSeqNum, fromInclusive = false))

        case (None, Some(toSeqNum)) =>  // a new shard has formed since `start` offset
          val fromSeqNum = getEarliestSeqNum(ss.streamName, ss.shardId)
          Some(SequenceNumberRange(ss.streamName, ss.shardId, fromSeqNum, toSeqNum, fromInclusive = true))

        case _ =>
          None
      }
    }
  }

  private def getInitialSeqNums(): Map[StreamAndShard, String] = {
    if (initialPositionInStream == InitialPositionInStream.TRIM_HORIZON) {
      streamNames.flatMap { streamName =>
        val desc = client.describeStream(streamName)
        desc.getStreamDescription.getShards.asScala.map { s =>
          val range = s.getSequenceNumberRange
          (StreamAndShard(streamName, s.getShardId), range.getStartingSequenceNumber)
        }
      }.toMap
    } else Map.empty
  }

  private def getEarliestSeqNum(streamName: String, shardId: String): String = {
    val desc = client.describeStream(streamName)
    val shard = desc.getStreamDescription.getShards.asScala.find(_.getShardId == shardId).get
    shard.getSequenceNumberRange.getStartingSequenceNumber
  }

  private def getLatestSeqNum(streamName: String, shardId: String): Option[String] = {
    println(s"\tgetting latest seq num for shard $shardId")

    val getShardIteratorRequest = new GetShardIteratorRequest()
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setShardIteratorType("LATEST")

    val getShardIteratorResult = client.getShardIterator(getShardIteratorRequest)
    val shardIterator = getShardIteratorResult.getShardIterator()

    val getRecordsRequest = new GetRecordsRequest()
    getRecordsRequest.setShardIterator(shardIterator)
    getRecordsRequest.setLimit(1)

    val getRecordsResult = client.getRecords(getRecordsRequest)
    val seqNum = getRecordsResult.getRecords().asScala.headOption.map { _.getSequenceNumber }
    println(s"\tlatest seq num = " + seqNum)
    seqNum
  }
}

private[kinesis] class KinesisRDDPartition(val seqNumRange: SequenceNumberRange, idx: Int) extends Partition {
  override def index: Int = idx
}

private[kinesis] class KinesisRDD(
    @transient sc: SparkContext,
    regionName: String,
    endpointUrl: String,
    @transient seqNumRanges: Seq[SequenceNumberRange],
    awsCredentialsOption: Option[SerializableAWSCredentials] = None,
    retryTimeoutMs: Int = 10000
  ) extends RDD[Array[Byte]](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    Array.tabulate(seqNumRanges.length) { i => new KinesisRDDPartition(seqNumRanges(i), i) }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val partition = split.asInstanceOf[KinesisRDDPartition]
    val credentials = awsCredentialsOption.getOrElse {
      new DefaultAWSCredentialsProviderChain().getCredentials()
    }
    new KinesisSequenceRangeIterator(credentials, endpointUrl, regionName,
      partition.seqNumRange, retryTimeoutMs).map(KinesisUtils.defaultMessageHandler)
  }
}

