package org.apache.spark.streaming.kinesis


import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import org.apache.spark.util.NextIterator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

private[kinesis]
class KinesisProxy(
    val credentialsProvider: AWSCredentialsProvider,
    val endpointUrl: String, val regionId: String) {

  val backoffTimeMillis = 1000
  val timeoutMillis = 10000

  lazy val client = new AmazonKinesisClient(credentialsProvider)
  client.setEndpoint(endpointUrl, "kinesis", regionId)


  def getStreamDescription(streamName: String, optionalStartingShardId: Option[String]): StreamDescription = {
    val describeStreamRequest = new DescribeStreamRequest
    describeStreamRequest.setStreamName(streamName)
    if (optionalStartingShardId.isDefined) {
      describeStreamRequest.setExclusiveStartShardId(optionalStartingShardId.get)
    }
    client.describeStream(describeStreamRequest).getStreamDescription()
  }

  def getAllShards(streamName: String): Set[Shard] = {
    val shards = new ArrayBuffer[Shard]()
    var done = false

    while (!done) {
      val description = getStreamDescription(streamName, shards.lastOption.map { _.getShardId })
      shards ++= description.getShards()
      done = false
      val status = description.getStreamStatus()
      if (status != StreamStatus.ACTIVE.toString()) {
        shards.clear()
        done = true
      } else if (!description.getHasMoreShards()) {
        done = true
      }
    }
    shards.toSet
  }

  def getRecordsAndNextShardIterator(
      streamName: String,
      shardId: String,
      shardIterator: String
    ): (Seq[Record], String) = {
    val getRecordsRequest = new GetRecordsRequest
    getRecordsRequest.setRequestCredentials(credentialsProvider.getCredentials)
    getRecordsRequest.setShardIterator(shardIterator)
    val getRecordsResult = client.getRecords(getRecordsRequest)
    (getRecordsResult.getRecords, getRecordsResult.getNextShardIterator)
  }

  def getShardIterator(
      streamName: String,
      shardId: String,
      iteratorType: ShardIteratorType,
      sequenceNumber: String
    ): String = {
    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setRequestCredentials(credentialsProvider.getCredentials)
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setShardIteratorType(iteratorType.toString)
    getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber)
    val getShardIteratorResult = client.getShardIterator(getShardIteratorRequest)
    getShardIteratorResult.getShardIterator
  }

  def getSequenceNumberRange(
      streamName: String,
      shardId: String,
      fromSeqNumber: String,
      toSeqNumber: String
    ): Iterator[Array[Byte]] = {
    new KinesisSequenceRangeIterator(this, streamName, shardId, fromSeqNumber, toSeqNumber)
  }
}

private class KinesisSequenceRangeIterator(
    proxy: KinesisProxy,
    streamName: String,
    shardId: String,
    fromSeqNumber: String,
    toSeqNumber: String
  ) extends NextIterator[Array[Byte]] {

  private var toSeqNumberReceived = false
  private var lastSeqNumber: String = null
  private var internalIterator: Iterator[Record] = null

  override protected def getNext(): Array[Byte] = {
    var nextBytes: Array[Byte] = null
    if (toSeqNumberReceived) {
      finished = true
    } else {

      if (internalIterator == null) {

        // If the internal iterator has not been initialized,
        // then fetch records from starting sequence number
        fetchRecords(ShardIteratorType.AT_SEQUENCE_NUMBER, fromSeqNumber)
      } else if (!internalIterator.hasNext) {

        // If the internal iterator does not have any more records,
        // then fetch more records after the last consumed sequence number
        fetchRecords(ShardIteratorType.AFTER_SEQUENCE_NUMBER, lastSeqNumber)
      }

      if (!internalIterator.hasNext) {

        // If the internal iterator still does not have any data, then throw exception
        // and terminate this iterator
        finished = true
        throw new Exception("Could not read until the specified end sequence number: " +
          s"shardId = $shardId, fromSequenceNumber = $fromSeqNumber, " +
          s"toSequenceNumber = $toSeqNumber")
      } else {

        // Get the record, and remember its sequence number
        val nextRecord = internalIterator.next()
        nextBytes = nextRecord.getData().array()
        lastSeqNumber = nextRecord.getSequenceNumber()

        // If the this record's sequence number matches the stopping sequence number, then make sure
        // the iterator is marked finished next time getNext() is called
        if (nextRecord.getSequenceNumber == toSeqNumber) {
          toSeqNumberReceived = true
        }
      }

    }
    nextBytes
  }

  override protected def close(): Unit = { }

  private def fetchRecords(iteratorType: ShardIteratorType, seqNum: String): Unit = {
    val shardIterator = proxy.getShardIterator(streamName, shardId, iteratorType, seqNum)
    var records: Seq[Record] = null
    do {
      try {
        val getResult = proxy.getRecordsAndNextShardIterator(streamName, shardId, shardIterator)
        records = getResult._1
      } catch {
        case ptee: ProvisionedThroughputExceededException =>
          Thread.sleep(proxy.backoffTimeMillis)
      }
    } while (records == null || records.length == 0)  // TODO: put a limit on the number of retries
    if (records != null && records.nonEmpty) {
      internalIterator = records.iterator
    }
  }
}