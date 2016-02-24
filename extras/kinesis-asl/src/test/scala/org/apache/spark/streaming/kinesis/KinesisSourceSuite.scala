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

abstract class KinesisSourceTest extends StreamTest with SharedSQLContext {

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
      if (sqlContext.sparkContext.isLocal) {
        // Only test this one in local mode so that we can assume there is only one BlockManager
        val streamBlocks =
          SparkEnv.get.blockManager.getMatchingBlockIds(_.isInstanceOf[StreamBlockId])
        val cleaned = streamBlocks.intersect(streamBlocksInLastBatch).isEmpty
        streamBlocksInLastBatch = streamBlocks.map(_.asInstanceOf[StreamBlockId])
        cleaned
      } else {
        true
      }
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

  test("DataFrameReader") {
    val df = sqlContext.read
      .option("endpoint", KinesisTestUtils.endpointUrl)
      .option("stream", "stream1")
      .option("accessKey", "accessKey")
      .option("secretKey", "secretKey")
      .option("position", InitialPositionInStream.TRIM_HORIZON.name())
      .kinesis().stream()

    val sources = df.queryExecution.analyzed.collect {
        case StreamingRelation(s: KinesisSource, _) => s
      }
    assert(sources.size === 1)

    // stream
    assertExceptionAndMessage[IllegalArgumentException](
      "Option 'stream' must be specified.") {
      sqlContext.read.kinesis().stream()
    }
    assertExceptionAndMessage[IllegalArgumentException](
      "Option 'stream' is invalid, as stream names cannot be empty.") {
      sqlContext.read.option("stream", "").kinesis().stream()
    }
    assertExceptionAndMessage[IllegalArgumentException](
      "Option 'stream' is invalid, as stream names cannot be empty.") {
      sqlContext.read.option("stream", "a,").kinesis().stream()
    }
    assertExceptionAndMessage[IllegalArgumentException](
      "Option 'stream' is invalid, as stream names cannot be empty.") {
      sqlContext.read.option("stream", ",a").kinesis().stream()
    }

    // region and endpoint
    // Setting either endpoint or region is fine
    sqlContext.read
      .option("stream", "stream1")
      .option("endpoint", KinesisTestUtils.endpointUrl)
      .option("accessKey", "accessKey")
      .option("secretKey", "secretKey")
      .kinesis().stream()
    sqlContext.read
      .option("stream", "stream1")
      .option("region", KinesisTestUtils.regionName)
      .option("accessKey", "accessKey")
      .option("secretKey", "secretKey")
      .kinesis().stream()

    assertExceptionAndMessage[IllegalArgumentException](
      "Either option 'region' or option 'endpoint' must be specified.") {
      sqlContext.read.option("stream", "stream1").kinesis().stream()
    }
    assertExceptionAndMessage[IllegalArgumentException](
      s"'region'(invalid-region) doesn't match to 'endpoint'(${KinesisTestUtils.endpointUrl})") {
      sqlContext.read
        .option("stream", "stream1")
        .option("region", "invalid-region")
        .option("endpoint", KinesisTestUtils.endpointUrl)
        .kinesis().stream()
    }

    // position
    assertExceptionAndMessage[IllegalArgumentException](
      "Unknown value of option 'position': invalid") {
      sqlContext.read
        .option("stream", "stream1")
        .option("endpoint", KinesisTestUtils.endpointUrl)
        .option("position", "invalid")
        .kinesis().stream()
    }

    // accessKey and secretKey
    assertExceptionAndMessage[IllegalArgumentException](
      "'accessKey' is set but 'secretKey' is not found") {
      sqlContext.read
        .option("stream", "stream1")
        .option("endpoint", KinesisTestUtils.endpointUrl)
        .option("position", InitialPositionInStream.TRIM_HORIZON.name())
        .option("accessKey", "test")
        .kinesis().stream()
    }
    assertExceptionAndMessage[IllegalArgumentException](
      "'secretKey' is set but 'accessKey' is not found") {
      sqlContext.read
        .option("stream", "stream1")
        .option("endpoint", KinesisTestUtils.endpointUrl)
        .option("position", InitialPositionInStream.TRIM_HORIZON.name())
        .option("secretKey", "test")
        .kinesis().stream()
    }
  }

  test("call kinesis when not using stream") {
    intercept[AnalysisException] {
      sqlContext.read.kinesis().load()
    }
  }

  private def assertExceptionAndMessage[T <: Exception : Manifest](
      expectedMessage: String)(body: => Unit): Unit = {
    val e = intercept[T] {
      body
    }
    assert(e.getMessage.contains(expectedMessage))
  }
}

class KinesisSourceStressTestSuite extends KinesisSourceTest with KinesisFunSuite {

  import testImplicits._

  testIfEnabled("kinesis source stress test") {
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
