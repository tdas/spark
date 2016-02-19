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
import scala.util.control.NonFatal

import com.amazonaws.AbortedException
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model._

import org.apache.spark._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Batch, Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockId, StorageLevel, StreamSourceBlockId}

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
          case (_, None) => -1   // old shard got eliminated by resharding
        }
      }

      val nonZeroSigns = comparisons.filter(_ != 0).toSet
      nonZeroSigns.size match {
        case 0 => 0 // if both empty or only 0s
        case 1 => nonZeroSigns.head // if there are only (0s and 1s) or (0s and -1s)
        case _ => // there are both 1s and -1s
          throw new IllegalArgumentException(
            s"Invalid comparison between non-linear histories: $this <=> $that")
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

  def nextBlockId: StreamSourceBlockId = StreamSourceBlockId("kinesis", nextId.getAndIncrement)
}

/**
 * However, this class runs in the driver so could be a bottleneck.
 */
private[kinesis] class KinesisDataFetcher(
    credentials: SerializableAWSCredentials,
    endpointUrl: String,
    regionName: String,
    fromSeqNums: Seq[(Shard, Option[String])],
    initialPositionInStream: InitialPositionInStream,
    readTimeoutMs: Long = 2000L
  ) extends Serializable with Logging {

  @transient private lazy val client = new AmazonKinesisClient(credentials)

  def fetch(sc: SparkContext): Array[(BlockId, SequenceNumberRange)] = {
    sc.makeRDD(fromSeqNums, fromSeqNums.size).map {
      case (shard, fromSeqNum) => fetchPartition(shard, fromSeqNum)
    }.collect().flatten
  }

  private def fetchPartition(
      shard: Shard,
      fromSeqNum: Option[String]): Option[(BlockId, SequenceNumberRange)] = {
    client.setEndpoint(endpointUrl, "kinesis", regionName)

    val endTime = System.currentTimeMillis + readTimeoutMs
    def timeLeft = math.max(endTime - System.currentTimeMillis, 0)

    val buffer = new ArrayBuffer[Array[Byte]]
    var firstSeqNumber: String = null
    var lastSeqNumber: String = fromSeqNum.orNull
    var lastIterator: String = null
    try {
      logDebug(s"Trying to fetch data from $shard, from seq num $lastSeqNumber")

      while (timeLeft > 0) {
        val (records, nextIterator) = retryOrTimeout("getting shard iterator", timeLeft) {
          if (lastIterator == null) {
            lastIterator = if (lastSeqNumber != null) {
              getKinesisIterator(shard, ShardIteratorType.AFTER_SEQUENCE_NUMBER, lastSeqNumber)
            } else {
              if (initialPositionInStream == InitialPositionInStream.LATEST) {
                getKinesisIterator(shard, ShardIteratorType.LATEST, lastSeqNumber)
              } else {
                getKinesisIterator(shard, ShardIteratorType.TRIM_HORIZON, lastSeqNumber)
              }
            }
          }
          getRecordsAndNextKinesisIterator(lastIterator)
        }

        records.foreach { record =>
          buffer += JavaUtils.bufferToArray(record.getData())
          if (firstSeqNumber == null) {
            firstSeqNumber = record.getSequenceNumber
          }
          lastSeqNumber = record.getSequenceNumber
        }

        lastIterator = nextIterator
      }

      if (buffer.nonEmpty) {
        val blockId = KinesisSource.nextBlockId
        SparkEnv.get.blockManager.putIterator(blockId, buffer.iterator, StorageLevel.MEMORY_ONLY)
        val range = SequenceNumberRange(
          shard.streamName, shard.shardId, firstSeqNumber, lastSeqNumber)
        logDebug(s"Received block $blockId having range $range from shard $shard")
        Some(blockId -> range)
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error fetching data from shard $shard", e)
        None
    }
  }

  /**
    * Get the records starting from using a Kinesis shard iterator (which is a progress handle
    * to get records from Kinesis), and get the next shard iterator for next consumption.
    */
  private def getRecordsAndNextKinesisIterator(
      shardIterator: String): (Seq[Record], String) = {
    val getRecordsRequest = new GetRecordsRequest().withShardIterator(shardIterator)
    getRecordsRequest.setRequestCredentials(credentials)
    val getRecordsResult = client.getRecords(getRecordsRequest)
    // De-aggregate records, if KPL was used in producing the records. The KCL automatically
    // handles de-aggregation during regular operation. This code path is used during recovery
    val records = UserRecord.deaggregate(getRecordsResult.getRecords)
    logTrace(
      s"Got ${records.size()} records and next iterator ${getRecordsResult.getNextShardIterator}")
    (records.asScala, getRecordsResult.getNextShardIterator)
  }

  /**
    * Get the Kinesis shard iterator for getting records starting from or after the given
    * sequence number.
    */
  private def getKinesisIterator(
      shard: Shard,
      iteratorType: ShardIteratorType,
      sequenceNumber: String): String = {
    val getShardIteratorRequest = new GetShardIteratorRequest()
      .withStreamName(shard.streamName)
      .withShardId(shard.shardId)
      .withShardIteratorType(iteratorType.toString)
      .withStartingSequenceNumber(sequenceNumber)
    getShardIteratorRequest.setRequestCredentials(credentials)
    val getShardIteratorResult = client.getShardIterator(getShardIteratorRequest)
    logTrace(s"Shard $shard: Got iterator ${getShardIteratorResult.getShardIterator}")
    getShardIteratorResult.getShardIterator
  }

  /** Helper method to retry Kinesis API request with exponential backoff and timeouts */
  private def retryOrTimeout[T](message: String, retryTimeoutMs: Long)(body: => T): T = {
    import KinesisSequenceRangeIterator._
    val startTimeMs = System.currentTimeMillis()
    var retryCount = 0
    var waitTimeMs = MIN_RETRY_WAIT_TIME_MS
    var result: Option[T] = None
    var lastError: Throwable = null

    def isTimedOut = (System.currentTimeMillis() - startTimeMs) >= retryTimeoutMs
    def isMaxRetryDone = retryCount >= MAX_RETRIES

    while (result.isEmpty && !isTimedOut && !isMaxRetryDone) {
      if (retryCount > 0) {
        // wait only if this is a retry
        Thread.sleep(waitTimeMs)
        waitTimeMs *= 2 // if you have waited, then double wait time for next round
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
          t match {
            case ptee: ProvisionedThroughputExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ptee)
            case e: Throwable =>
              throw new SparkException(s"Error while $message", e)
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      if (isTimedOut) {
        throw new SparkException(
          s"Timed out after $retryTimeoutMs ms while $message, last exception: ", lastError)
      } else {
        throw new SparkException(
          s"Gave up after $retryCount retries while $message, last exception: ", lastError)
      }
    }
  }
}
