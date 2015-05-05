package org.apache.spark.streaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging

object KinesisTestUtils extends Logging {

  val enableTests = true
  val streamName = "TDTest"
  val regionId = "us-east-1"
  val endPointUrl = "https://kinesis.us-east-1.amazonaws.com"

  lazy val kinesisClient = {
    val client = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    client.setEndpoint(endPointUrl)
    client
  }

  /**
   * Push data to Kinesis stream and return a map of
   *  shardId -> seq of (data, seq number) pushed to corresponding shard
   */
  def pushData(testData: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()

    testData.foreach { num =>
      val str = num.toString
      val putRecordRequest = new PutRecordRequest().withStreamName(streamName)
        .withData(ByteBuffer.wrap(str.getBytes()))
        .withPartitionKey(str)

      val putRecordResult = kinesisClient.putRecord(putRecordRequest)
      val shardId = putRecordResult.getShardId
      val seqNumber = putRecordResult.getSequenceNumber()
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(Int, String)]())
      sentSeqNumbers += ((num, seqNumber))

    }
    logDebug(s"Pushed $testData:\n\t ${shardIdToSeqNumbers.mkString("\n\t")}")
    shardIdToSeqNumbers.toMap
  }
}
