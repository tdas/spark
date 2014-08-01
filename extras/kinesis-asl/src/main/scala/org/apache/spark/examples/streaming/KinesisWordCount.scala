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

package org.apache.spark.examples.streaming

import java.nio.ByteBuffer

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kinesis.KinesisUtils

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest

/**
 * Kinesis Spark Streaming WordCount example.
 *
 * See http://spark.apache.org/docs/latest/streaming-programming-guide.html for more details on
 *   the Kinesis Spark Streaming integration.
 *
 * This example spins up 1 Kinesis Worker (Spark Streaming Receivers) per shard of the
 *   given stream.
 * It then starts pulling from the last checkpointed sequence number of the given 
 *   <stream-name> and <endpoint-url>. 
 *
 * Valid endpoint urls:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
 * 
 * This code uses the DefaultAWSCredentialsProviderChain and searches for credentials
 *   in the following order of precedence:
 * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 * Java System Properties - aws.accessKeyId and aws.secretKey
 * Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 * Instance profile credentials - delivered through the Amazon EC2 metadata service
 *
 * Usage: KinesisWordCount <stream-name> <endpoint-url>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <endpoint-url> is the endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/extras/kinesis-asl/bin/run-kinesis-example \
 *        org.apache.spark.examples.streaming.KinesisWordCount mySparkStream \
 *        https://kinesis.us-east-1.amazonaws.com
 *
 * There is a companion helper class below called KinesisWordCountProducer which puts
 *   dummy data onto the Kinesis stream.
 * Usage instructions for KinesisWordCountProducer are provided in that class definition.
 */
object KinesisWordCount {

  def main(args: Array[String]) {

    // Check that all required args were passed in.
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: KinesisWordCount <stream-name> <endpoint-url>
          |    <stream-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the Kinesis service
          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
        """.stripMargin)
      System.exit(1)
    }

    // Populate the appropriate variables from the given args
    val Array(stream, endpoint) = args

    // Determine the number of shards from the stream
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpoint)
    val numShards = kinesisClient.describeStream(stream).getStreamDescription().getShards().size()

    // In this example, we're going to create 1 Kinesis Worker/Receiver/DStream for each shard.
    val numStreams = numShards

    // 1 more thread than the number of receivers, to have thread for processing data
    val numSparkThreads = numStreams + 1

    // Setup the StreamingContext
    val appName = "KinesisWordCount"
    val batchInterval = Milliseconds(2000)          // Spark Streaming batch interval
    val kinesisCheckpointInterval = batchInterval   // Kinesis checkpoint interval
    val sparkConfig = new SparkConf().setAppName(appName).setMaster(s"local[$numSparkThreads]")
    val ssc = new StreamingContext(sparkConfig, batchInterval)

    // Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards
    val kafkaStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, appName, stream,
        endpoint, kinesisCheckpointInterval.milliseconds, InitialPositionInStream.LATEST,
        StorageLevel.MEMORY_AND_DISK)
    }

    // Union all the streams
    val allStreams = ssc.union(kafkaStreams)  // union all the streams

    // Convert each lines of Array[Byte] to String, then split into words, and counts them
    val words = allStreams.flatMap(byteArray => new String(byteArray).split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * Usage: KinesisWordCountProducer <stream-name> <kinesis-endpoint-url>
 *     <recordsPerSec> <wordsPerRecord>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <kinesis-endpoint-url> is the endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *   <records-per-sec> is the rate of records per second to put onto the stream
 *   <words-per-record> is the rate of records per second to put onto the stream
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/extras/kinesis-asl/bin/run-kinesis-example \
 *         org.apache.spark.examples.streaming.KinesisWordCountProducer mySparkStream \
 *         https://kinesis.us-east-1.amazonaws.com 10 5
 */
object KinesisWordCountProducer {

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: KinesisWordCountProducer <stream-name> <endpoint-url>" +
          " <records-per-sec> <words-per-record>")
      System.exit(1)
    }

    /** Populate the appropriate variables from the given args */
    val Array(stream, endpoint, recordsPerSecond, wordsPerRecord) = args

    /** Generate the records and return the totals */
    val totals = generate(stream, endpoint, recordsPerSecond.toInt, wordsPerRecord.toInt)

    println("Totals")
    totals.foreach(total => println(total.toString()))
  }

  def generate(stream: String,
      endpoint: String,
      recordsPerSecond: Int,
      wordsPerRecord: Int): Seq[(Int, Int)] = {

    val maxRandomInts = 10

    /** Create the Kinesis client */
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpoint)

    println(s"Putting records onto stream $stream and endpoint $endpoint at a rate of" +
        s" $recordsPerSecond records per second and $wordsPerRecord words per record");

    val totals = new Array[Int](maxRandomInts)

    /** Put String records onto the stream per the given recordPerSec and wordsPerRecord */
    for (i <- 1 to 5) {

      /** Generate recordsPerSec records to put onto the stream */
      val records = (1 to recordsPerSecond.toInt).map { recordNum =>
        /** 
         *  Randomly generate each wordsPerRec words between 0 (inclusive)
         *  and MAX_RANDOM_INTS (exclusive) 
         */
        val data = (1 to wordsPerRecord.toInt).map(x => {
          val randomInt = Random.nextInt(maxRandomInts)
          totals(randomInt) += 1
          randomInt.toString()
        }).mkString(" ")

        /** Create a partitionKey based on recordNum */
        val partitionKey = s"partitionKey-$recordNum"

        /** Create a PutRecordRequest with an Array[Byte] version of the data */
        val putRecordRequest = new PutRecordRequest().withStreamName(stream)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes()));

        /** Put the record onto the stream and capture the PutRecordResult */
        val putRecordResult = kinesisClient.putRecord(putRecordRequest);
      }

      /** Sleep for a second */
      Thread.sleep(1000)
      println("Sent " + recordsPerSecond + " records")
    }

    /** Convert the totals to (index, total) tuple */
    (0 to (maxRandomInts - 1)).zip(totals)
  }
}
