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

package org.apache.spark.sql.execution.streaming

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.execution.streaming.RangeStreamSource._
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamTest}
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.ManualClock

class RangeStreamSourceSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._

  after {
    spark.streams.active.foreach(_.stop)
  }

  testWithUninterruptibleThread("FixedRate strategy: without end") {
    withTempDir { metadataDir =>
      val manualClock = new ManualClock
      val source = RangeStreamSource(
        sqlContext,
        range = DataRange(10, None),
        strategy = FixedRate(10),
        numPartitions = 2,
        metadataPath = metadataDir.getAbsolutePath,
        clock = manualClock)

      assert(source.getOffset === Some(LongOffset(10)))
      val batch0 = source.getBatch(None, LongOffset(0))
      assert(batch0.count === 0)
      assert(batch0.queryExecution.toRdd.getNumPartitions === 2)

      manualClock.advance(2000) // 2 secs
      assert(source.getOffset === Some(LongOffset(30)))
      val batch1 = source.getBatch(Some(LongOffset(10)), LongOffset(30))
      assert(batch1.as[Long].collect().toSeq === (10 until 30))
      assert(batch1.queryExecution.toRdd.getNumPartitions === 2)

      // Reached the end
      manualClock.advance(3000) // 3 secs
      assert(source.getOffset === Some(LongOffset(60)))
      val batch2 = source.getBatch(Some(LongOffset(30)), LongOffset(60))
      assert(batch2.as[Long].collect().toSeq === (30 until 60))
      assert(batch2.queryExecution.toRdd.getNumPartitions === 2)
    }
  }

  testWithUninterruptibleThread("FixedRate strategy: with end") {
    withTempDir { metadataDir =>
      val manualClock = new ManualClock
      val source = RangeStreamSource(
        sqlContext,
        range = DataRange(10, Option(50)),
        strategy = FixedRate(10),
        numPartitions = 2,
        metadataPath = metadataDir.getAbsolutePath,
        clock = manualClock)

      assert(source.getOffset === Some(LongOffset(10)))
      val batch0 = source.getBatch(None, LongOffset(0))
      assert(batch0.count === 0)
      assert(batch0.queryExecution.toRdd.getNumPartitions === 2)

      // Reached the end
      manualClock.advance(5000) // 5 secs
      assert(source.getOffset === Some(LongOffset(50)))
      val batch2 = source.getBatch(Some(LongOffset(10)), LongOffset(50))
      assert(batch2.as[Long].collect().toSeq === (10 until 50))
      assert(batch2.queryExecution.toRdd.getNumPartitions === 2)
    }
  }

  testWithUninterruptibleThread("FixedCount strategy: without end") {
    withTempDir { metadataDir =>
      val manualClock = new ManualClock
      val source = RangeStreamSource(
        sqlContext,
        range = DataRange(10, None),
        strategy = FixedCount(10),
        numPartitions = 2,
        metadataPath = metadataDir.getAbsolutePath,
        clock = manualClock)

      assert(source.getOffset === Some(LongOffset(10)))
      val batch0 = source.getBatch(None, LongOffset(10))
      assert(batch0.count === 0)
      assert(batch0.queryExecution.toRdd.getNumPartitions === 2)

      assert(source.getOffset === Some(LongOffset(20)))
      val batch1 = source.getBatch(Some(LongOffset(10)), LongOffset(20))
      assert(batch1.as[Long].collect().toSeq === (10 until 20))
      assert(batch1.queryExecution.toRdd.getNumPartitions === 2)

      assert(source.getOffset === Some(LongOffset(30)))
      val batch2 = source.getBatch(Some(LongOffset(20)), LongOffset(30))
      assert(batch2.as[Long].collect().toSeq === (20 until 30))
      assert(batch2.queryExecution.toRdd.getNumPartitions === 2)
    }
  }

  testWithUninterruptibleThread("FixedCount strategy: with end") {
    withTempDir { metadataDir =>
      val manualClock = new ManualClock
      val source = RangeStreamSource(
        sqlContext,
        range = DataRange(10, Option(25)),
        strategy = FixedCount(10),
        numPartitions = 2,
        metadataPath = metadataDir.getAbsolutePath,
        clock = manualClock)

      assert(source.getOffset === Some(LongOffset(10)))
      val batch0 = source.getBatch(None, LongOffset(10))
      assert(batch0.count === 0)
      assert(batch0.queryExecution.toRdd.getNumPartitions === 2)

      assert(source.getOffset === Some(LongOffset(20)))
      val batch1 = source.getBatch(Some(LongOffset(10)), LongOffset(20))
      assert(batch1.as[Long].collect().toSeq === (10 until 20))
      assert(batch1.queryExecution.toRdd.getNumPartitions === 2)

      assert(source.getOffset === Some(LongOffset(25)))
      val batch2 = source.getBatch(Some(LongOffset(20)), LongOffset(25))
      assert(batch2.as[Long].collect().toSeq === (20 until 25))
      assert(batch2.queryExecution.toRdd.getNumPartitions === 2)
    }
  }

  test("real-time test") {
    val random = spark.readStream
      .format("range")
      .option("start", "100")
      .option("end", "200")
      .option("fixedRate", "27")
      .load()

    val counts = random.groupBy($"id" % 10).count()

    val counter = new StreamingQueryListener {
      @volatile var numRowsProcessed: Long = 0L
      override def onQueryProgress(queryProgress: QueryProgress): Unit = {
        val status = queryProgress.queryStatus
        val numInputRows = status.triggerStatus.asScala.getOrElse("numRows.input.total", "0").toLong
        numRowsProcessed += numInputRows
      }
      override def onQueryStarted(queryStarted: QueryStarted): Unit = {}
      override def onQueryTerminated(queryTerminated: QueryTerminated): Unit = {}

    }
    try {
      spark.streams.addListener(counter)
      val query = counts.writeStream
        .format("memory")
        .queryName("table")
        .outputMode("complete")
        .start()
      query.processAllAvailable()
      assert(counter.numRowsProcessed === 100)
      assert(spark.table("table").as[(Long, Long)].collect().toSet === (0 to 9).map (_ -> 10).toSet)
      query.stop()
    } finally {
      spark.streams.removeListener(counter)
    }
  }
}

