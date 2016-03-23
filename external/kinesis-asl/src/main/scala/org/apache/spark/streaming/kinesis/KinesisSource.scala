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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.{BlockId, StreamBlockId}
import org.apache.spark.util.ThreadUtils

private[kinesis] case class Shard(streamName: String, shardId: String)

private[kinesis] object Shard {
  def apply(range: SequenceNumberRange): Shard = Shard(range.streamName, range.shardId)
}

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
    initialPosInStream: InitialPositionInStream,
    awsCredentials: SerializableAWSCredentials) extends Source with Logging {

  // How long we should wait before calling `fetchShards()`. Because DescribeStream has a limit of
  // 10 transactions per second per account, we should not request too frequently.
  private val FETCH_SHARDS_INTERVAL_MS = 200L

  private val shards = new mutable.HashSet[Shard]
  private val latestSeqNum = new mutable.HashMap[Shard, String]

  private val prefetchedData =
    new mutable.HashMap[Shard, ArrayBuffer[(SequenceNumberRange, BlockId)]]

  private val prefetchingService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("kinesis-source")

  private val client = new AmazonKinesisClient(awsCredentials)
  client.setEndpoint(endpointUrl)

  implicit private val encoder = ExpressionEncoder[Array[Byte]]

  private var prefetchingTask: Runnable = null

  override val schema: StructType = encoder.schema

  override def getOffset: Option[Offset] = synchronized {
    startPrefetchingIfNeeded()
    fetchAndUpdateShards()
    if (shards.forall(latestSeqNum.contains)) {
      val offsets = shards.map(s => s -> latestSeqNum(s))
      logInfo(s"Offsets:\n\t${offsets.mkString("\n\t")}")
      Some(KinesisSourceOffset(offsets.toMap))
    } else {
      logInfo(s"Offsets: None!!!!")
      None
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    logInfo(s"Get batch called with $start and $end")
    startPrefetchingIfNeeded()
    updateShardsAndLatestSeqNums(end.asInstanceOf[KinesisSourceOffset].seqNums)

    val rangesAndBlockIds = end.asInstanceOf[KinesisSourceOffset].seqNums.keys.flatMap { shard =>
      val startSeqNumOption = start.flatMap(_.asInstanceOf[KinesisSourceOffset].seqNums.get(shard))
      val endSeqNum = end.asInstanceOf[KinesisSourceOffset].seqNums(shard)
      val queue = prefetchedData.getOrElseUpdate(shard, new ArrayBuffer)
      val startIndex = startSeqNumOption match {
        case Some(startSeqNum) =>
          val idx = queue.indexWhere(_._1.toSeqNumber == startSeqNum) // -1 if it doesn't find
          if (idx == -1) -1 else idx + 1 // we should start reading from the next block
        case None =>
          0
      }
      val endIndex = queue.indexWhere(_._1.toSeqNumber == endSeqNum) // -1 if it doesn't find
      if (startIndex >= 0 && endIndex >= 0 && startIndex <= endIndex) {
        queue.slice(startIndex, endIndex + 1)
      } else {
        startSeqNumOption match {
          case Some(startSeqNum) if (startSeqNum != endSeqNum) =>
            val range = SequenceNumberRange(
              shard.streamName,
              shard.shardId,
              startSeqNum,
              endSeqNum,
              fromInclusive = false)
            Seq(range -> KinesisSource.nonExistentBlockId)
          case _ =>
            None
        }
      }
    }.toArray
    logInfo(s"Batch:\n\t${rangesAndBlockIds.mkString("\n\t")}")
    val ranges = rangesAndBlockIds.map(_._1)
    val blockIds = rangesAndBlockIds.map(_._2)
    val rdd = new KinesisBackedBlockRDD[Array[Byte]](
      sqlContext.sparkContext, regionName,
      endpointUrl, blockIds, ranges.map(SequenceNumberRanges.apply))
    sqlContext.createDataset(rdd).toDF
  }

  private def prefetchData(): Unit = {
    fetchAndUpdateShards()
    val fromSeqNumsWithBlockId = synchronized {
      shards.map { shard =>
        val lastPrefetchedSeqNum =
          prefetchedData.get(shard).flatMap(_.lastOption.map(_._1.toSeqNumber))
        val startSeqNum = lastPrefetchedSeqNum.orElse(latestSeqNum.get(shard))
        (shard, startSeqNum, KinesisSource.nextBlockId)
      }.toSeq

    }
    val fetcher = new KinesisDataFetcher(
      awsCredentials, endpointUrl, fromSeqNumsWithBlockId, initialPosInStream)
    val fetchedData = fetcher.fetch(sqlContext.sparkContext)
    logInfo(s"Fetched data:\n\t${fetchedData.mkString("\n\t")}")

    synchronized {
      fetchedData.foreach { case (blockId, seqNumRange) =>
        val shard = Shard(seqNumRange)
        val queue = prefetchedData.getOrElseUpdate(shard, new ArrayBuffer)
        queue += seqNumRange -> blockId
      }
      updateShardsAndLatestSeqNums(fetchedData.map(x => (x._2.shard, x._2.toSeqNumber)).toMap)
    }
  }

  private def fetchAndUpdateShards(): Unit = synchronized {
    try {
      shards ++= streamNames.toSeq.flatMap { streamName =>
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

  private def updateShardsAndLatestSeqNums(newSeqNums: Map[Shard, String]): Unit = synchronized {
    shards ++= newSeqNums.keys

    newSeqNums.foreach { case (shard, newSeqNum) =>
      if (!latestSeqNum.contains(shard) || BigInt(latestSeqNum(shard)) < BigInt(newSeqNum)) {
        latestSeqNum.put(shard, newSeqNum)
      }
    }
  }

  private def startPrefetchingIfNeeded(): Unit = synchronized {
    if (prefetchingTask == null) {
      prefetchingTask = new Runnable() {
        override def run(): Unit = {
          prefetchData()
        }
      }
      prefetchingService.scheduleWithFixedDelay(
        prefetchingTask, FETCH_SHARDS_INTERVAL_MS, FETCH_SHARDS_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def toString: String = s"KinesisSource[streamNames=${streamNames.mkString(",")}]"
}

private[kinesis] object KinesisSource {

  private val nextId = new AtomicLong(0)
  private val nonExistentBlockId = nextBlockId

  def nextBlockId: StreamBlockId = StreamBlockId(Int.MaxValue, nextId.getAndIncrement)
}
