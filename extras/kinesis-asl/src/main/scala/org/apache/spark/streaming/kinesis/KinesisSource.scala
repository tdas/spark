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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.AbortedException
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Batch, Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockId, StreamBlockId}

private[kinesis] case class Shard(streamName: String, shardId: String)

private[kinesis] case class KinesisSourceOffset(seqNums: Map[Shard, String])
  extends Offset {

  override def compareTo(otherOffset: Offset): Int = otherOffset match {
    case that: KinesisSourceOffset =>
      val allShards = this.seqNums.keySet ++ that.seqNums.keySet
      val comparisons = allShards.map { shard =>
        (this.seqNums.get(shard).map(BigInt(_)), that.seqNums.get(shard).map(BigInt(_))) match {
          case (Some(thisNum), Some(thatNum)) => thisNum.compare(thatNum)
          case (None, _) => -1   // new shard started by resharding
          case (_, None) => 1   // old shard got eliminated by resharding
        }
      }

      val nonZeroSigns = comparisons.filter(_ != 0).toSet
      nonZeroSigns.size match {
        case 0 => 0 // if both empty or only 0s
        case 1 => nonZeroSigns.head // if there are only (0s and 1s) or (0s and -1s)
        case _ => // there are both 1s and -1s
          throw new IllegalArgumentException(
            s"Invalid comparison between KinesisSource offsets: \n\t this: $this\n\t that: $that")
      }

    case _ =>
      throw new IllegalArgumentException(s"Cannot compare $this <=> $otherOffset")
  }
}

private[kinesis] class KinesisSource(
    sqlContext: SQLContext,
    regionName: String,
    endpointUrl: String,
    streamNames: Set[String],
    initialPosInStream: InitialPositionInStream = InitialPositionInStream.LATEST,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None) extends Source {

  // How long we should wait before calling `fetchShards()`. Because DescribeStream has a limit of
  // 10 transactions per second per account, we should not request too frequently.
  private val FETCH_SHARDS_INTERVAL_MS = 200L

  // The last time `fetchShards()` is called.
  private var lastFetchShardsTimeMS = 0L

  implicit private val encoder = ExpressionEncoder[Array[Byte]]

  private val credentials = SerializableAWSCredentials(
    awsCredentialsOption.getOrElse(new DefaultAWSCredentialsProviderChain().getCredentials())
  )

  private val client = new AmazonKinesisClient(credentials)
  client.setEndpoint(endpointUrl, "kinesis", regionName)

  private var cachedBlocks = new mutable.HashSet[BlockId]

  override val schema: StructType = encoder.schema

  override def getNextBatch(start: Option[Offset]): Option[Batch] = {
    val startOffset = start.map(_.asInstanceOf[KinesisSourceOffset])

    val now = System.currentTimeMillis()
    if (now - lastFetchShardsTimeMS < FETCH_SHARDS_INTERVAL_MS) {
      // Because DescribeStream has a limit of 10 transactions per second per account, we should not
      // request too frequently.
      return None
    }
    lastFetchShardsTimeMS = now
    val shards = fetchShards()

    // Get the starting seq number of each shard if available
    val fromSeqNums = shards.map { shard => shard -> startOffset.flatMap(_.seqNums.get(shard)) }

    /** Prefetch Kinesis data from the starting seq nums */
    val prefetchedData = new KinesisDataFetcher(
      credentials,
      endpointUrl,
      regionName,
      fromSeqNums,
      initialPosInStream).fetch(sqlContext.sparkContext)

    if (prefetchedData.nonEmpty) {
      val prefetechedRanges = prefetchedData.map(_._2)
      val prefetchedBlockIds = prefetchedData.map(_._1)

      val fromSeqNumsMap = fromSeqNums.filter(_._2.nonEmpty).toMap.mapValues(_.get)
      val toSeqNumsMap = prefetechedRanges
        .map(x => Shard(x.streamName, x.shardId) -> x.toSeqNumber)
        .toMap
      val endOffset = shards.flatMap { shard =>
        val shardEndSeqNum = toSeqNumsMap.get(shard).orElse(fromSeqNumsMap.get(shard)).orNull
        if (shardEndSeqNum != null) Some(shard -> shardEndSeqNum) else None
      }.toMap

      val rdd = new KinesisBackedBlockRDD[Array[Byte]](sqlContext.sparkContext, regionName,
        endpointUrl, prefetchedBlockIds, prefetechedRanges.map(SequenceNumberRanges.apply))

      dropOldBlocks()
      cachedBlocks ++= prefetchedBlockIds

      Some(new Batch(new KinesisSourceOffset(endOffset), sqlContext.createDataset(rdd).toDF))
    } else {
      None
    }
  }

  private def dropOldBlocks(): Unit = {
    val droppedBlocks = ArrayBuffer[BlockId]()
    try {
      for (blockId <- cachedBlocks) {
        SparkEnv.get.blockManager.removeBlock(blockId)
        droppedBlocks += blockId
      }
    } finally {
      cachedBlocks --= droppedBlocks
    }
  }

  private def fetchShards(): Seq[Shard] = {
    try {
      streamNames.toSeq.flatMap { streamName =>
        val desc = client.describeStream(streamName)
        desc.getStreamDescription.getShards.asScala.map { s =>
          Shard(streamName, s.getShardId)
        }
      }
    } catch {
      case e: AbortedException =>
        // AbortedException will be thrown if the current thread is interrupted
        // So let's convert it back to InterruptedException
        val e1 = new InterruptedException("thread is interrupted")
        e1.addSuppressed(e)
        throw e1
    }
  }
}

private[kinesis] object KinesisSource {

  private val nextId = new AtomicLong(0)

  def nextBlockId: StreamBlockId = StreamBlockId(-1, nextId.getAndIncrement)
}
