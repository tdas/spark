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
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.test.SharedSQLContext


class KinesisSourceSuite extends StreamTest with SharedSQLContext with KinesisFunSuite {

  import testImplicits._

  private var testUtils: KPLBasedKinesisTestUtils = _
  private var streamName: String = _

  override val streamingTimout = 60.seconds

  case class AddKinesisData(kinesisSource: KinesisSource, data: Int*) extends AddData {
    override def addData(): Offset = {
      val shardIdToSeqNums = testUtils.pushData(data, false).map { case (shardId, info) =>
        (Shard(streamName, shardId), info.last._2)
      }
      assert(shardIdToSeqNums.size === testUtils.streamShardCount,
        s"Data must be send to all ${testUtils.streamShardCount} shards of stream $streamName")
      KinesisSourceOffset(shardIdToSeqNums)
    }

    override def source: Source = kinesisSource
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KPLBasedKinesisTestUtils
    testUtils.createStream()
    streamName = testUtils.streamName
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.deleteStream()
    }
    super.afterAll()
  }

  test("basic receiving") {
    val kinesisSource = KinesisSource(
      sqlContext,
      testUtils.regionName,
      testUtils.endpointUrl,
      Set(streamName),
      InitialPositionInStream.TRIM_HORIZON)
    val mapped = kinesisSource.toDS().map[Int]((bytes: Array[Byte]) => new String(bytes).toInt + 1)
    val testData = 1 to 10 // This ensures that data is sent to multiple shards for 2 shard streams
    testStream(mapped)(
      AddKinesisData(kinesisSource, testData: _*),
      CheckAnswer(testData.map { _ + 1 }: _*)
    )
  }
}