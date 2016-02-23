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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{AnalysisException, StreamTest}
import org.apache.spark.sql.execution.streaming.{Offset, Source, StreamingRelation}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.storage.StreamBlockId

class KinesisSourceTest extends StreamTest with SharedSQLContext {

  case class AddKinesisData(
      testUtils: KPLBasedKinesisTestUtils,
      kinesisSource: KinesisSource,
      data: Seq[Int]) extends AddData {

    override def addData(): Offset = {
      testUtils.pushData(data, false)
      val shardIdToSeqNums = testUtils.getLatestSeqNumsOfShards().map { case (shardId, seqNum) =>
        (Shard(testUtils.streamName, shardId), seqNum)
      }
      KinesisSourceOffset(shardIdToSeqNums)
    }

    override def source: Source = kinesisSource
  }

  def createKinesisSourceForTest(testUtils: KPLBasedKinesisTestUtils): KinesisSource = {
    new KinesisSource(
      sqlContext,
      testUtils.regionName,
      testUtils.endpointUrl,
      Set(testUtils.streamName),
      InitialPositionInStream.TRIM_HORIZON,
      SerializableAWSCredentials(KinesisTestUtils.getAWSCredentials()))
  }
}

class KinesisSourceSuite extends KinesisSourceTest with KinesisFunSuite {

  import testImplicits._

  testIfEnabled("basic receiving and failover") {
    var streamBlocksInLastBatch: Seq[StreamBlockId] = Seq.empty

    def assertStreamBlocks: Boolean = {
      // Assume the test runs in local mode and there is only one BlockManager.
      val streamBlocks =
        SparkEnv.get.blockManager.getMatchingBlockIds(_.isInstanceOf[StreamBlockId])
      val cleaned = streamBlocks.intersect(streamBlocksInLastBatch).isEmpty
      streamBlocksInLastBatch = streamBlocks.map(_.asInstanceOf[StreamBlockId])
      cleaned
    }

    val testUtils = new KPLBasedKinesisTestUtils
    testUtils.createStream()
    try {
      val kinesisSource = createKinesisSourceForTest(testUtils)
      val mapped =
        kinesisSource.toDS[Array[Byte]]().map((bytes: Array[Byte]) => new String(bytes).toInt + 1)
      val testData = 1 to 10
      testStream(mapped)(
        AddKinesisData(testUtils, kinesisSource, testData),
        CheckAnswer((1 to 10).map(_ + 1): _*),
        Assert(assertStreamBlocks, "Old stream blocks should be cleaned"),
        StopStream,
        AddKinesisData(testUtils, kinesisSource, 11 to 20),
        StartStream,
        CheckAnswer((1 to 20).map(_ + 1): _*),
        Assert(assertStreamBlocks, "Old stream blocks should be cleaned"),
        AddKinesisData(testUtils, kinesisSource, 21 to 30),
        CheckAnswer((1 to 30).map(_ + 1): _*),
        Assert(assertStreamBlocks, "Old stream blocks should be cleaned")
      )
    } finally {
      testUtils.deleteStream()
    }
  }

  testIfEnabled("DataFrameReader") {
    val testUtils = new KPLBasedKinesisTestUtils
    testUtils.createStream()
    try {
      val df = sqlContext.read
        .option("region", testUtils.regionName)
        .option("endpoint", testUtils.endpointUrl)
        .option("streams", testUtils.streamName)
        .option("initialPosInStream", InitialPositionInStream.TRIM_HORIZON.name())
        .kinesis().stream()

      val sources = df.queryExecution.analyzed.collect {
          case StreamingRelation(s: KinesisSource, _) => s
        }
      assert(sources.size === 1)

      // streams
      assertExceptionAndMessage[IllegalArgumentException]("Option 'streams' is not specified") {
        sqlContext.read.kinesis().stream()
      }
      assertExceptionAndMessage[IllegalArgumentException](
        "Option 'streams' is invalid. Please use comma separated string (e.g., 'stream1,stream2')"
      ) {
        sqlContext.read.option("streams", "").kinesis().stream()
      }
      assertExceptionAndMessage[IllegalArgumentException](
        "Option 'streams' is invalid. Please use comma separated string (e.g., 'stream1,stream2')"
      ) {
        sqlContext.read.option("streams", "a,").kinesis().stream()
      }
      assertExceptionAndMessage[IllegalArgumentException](
        "Option 'streams' is invalid. Please use comma separated string (e.g., 'stream1,stream2')"
      ) {
        sqlContext.read.option("streams", ",a").kinesis().stream()
      }

      // region and endpoint
      // Setting either endpoint or region is fine
      sqlContext.read
        .option("streams", testUtils.streamName)
        .option("endpoint", testUtils.endpointUrl)
        .kinesis().stream()
      sqlContext.read
        .option("streams", testUtils.streamName)
        .option("region", testUtils.regionName)
        .kinesis().stream()

      assertExceptionAndMessage[IllegalArgumentException](
        "Either 'region' or 'endpoint' should be specified") {
        sqlContext.read.option("streams", testUtils.streamName).kinesis().stream()
      }
      assertExceptionAndMessage[IllegalArgumentException](
        s"'region'(invalid-region) doesn't match to 'endpoint'(${testUtils.endpointUrl})") {
        sqlContext.read
          .option("streams", testUtils.streamName)
          .option("region", "invalid-region")
          .option("endpoint", testUtils.endpointUrl)
          .kinesis().stream()
      }

      // initialPosInStream
      assertExceptionAndMessage[IllegalArgumentException](
        "Unknown value of option initialPosInStream: invalid") {
        sqlContext.read
          .option("streams", testUtils.streamName)
          .option("endpoint", testUtils.endpointUrl)
          .option("initialPosInStream", "invalid")
          .kinesis().stream()
      }

      // accessKey and secretKey
      assertExceptionAndMessage[IllegalArgumentException](
        "'accessKey' is set but 'secretKey' is not found") {
        sqlContext.read
          .option("streams", testUtils.streamName)
          .option("endpoint", testUtils.endpointUrl)
          .option("initialPosInStream", InitialPositionInStream.TRIM_HORIZON.name())
          .option("accessKey", "test")
          .kinesis().stream()
      }
      assertExceptionAndMessage[IllegalArgumentException](
        "'secretKey' is set but 'accessKey' is not found") {
        sqlContext.read
          .option("streams", testUtils.streamName)
          .option("endpoint", testUtils.endpointUrl)
          .option("initialPosInStream", InitialPositionInStream.TRIM_HORIZON.name())
          .option("secretKey", "test")
          .kinesis().stream()
      }
    } finally {
      testUtils.deleteStream()
    }
  }

  testIfEnabled("call kinesis when not using stream") {
    val expectedMessage = "org.apache.spark.streaming.kinesis.DefaultSource is " +
      "neither a RelationProvider nor a FSBasedRelationProvider.;"
    assertExceptionAndMessage[AnalysisException](expectedMessage) {
      sqlContext.read.kinesis().load()
    }
  }

  private def assertExceptionAndMessage[T <: Exception : Manifest](
      expectedMessage: String)(body: => Unit): Unit = {
    val e = intercept[T] {
      body
    }
    assert(e.getMessage === expectedMessage)
  }
}

class KinesisSourceStressTestSuite extends KinesisSourceTest with KinesisFunSuite {

  import testImplicits._

  test("kinesis source stress test") {
    val testUtils = new KPLBasedKinesisTestUtils
    testUtils.createStream()
    try {
      val kinesisSource = createKinesisSourceForTest(testUtils)
      val ds = kinesisSource.toDS[String]().map(_.toInt + 1)
      runStressTest(ds, data => {
        AddKinesisData(testUtils, kinesisSource, data)
      })
    } finally {
      testUtils.deleteStream()
    }
  }
}
