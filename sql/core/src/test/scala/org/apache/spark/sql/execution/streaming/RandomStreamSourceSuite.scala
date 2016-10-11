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
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamTest}
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.ManualClock

class RandomStreamSourceSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._
  private val manualClock = new ManualClock

  after {
    spark.streams.active.foreach(_.stop)
  }

  test("basic functionality") {
    val source = new RandomStreamSource(sqlContext, 100, 10, 2, manualClock)
    assert(source.getOffset === Some(LongOffset(0)))
    val batch0 = source.getBatch(None, LongOffset(0))
    assert(batch0.count === 0)
    assert(batch0.queryExecution.toRdd.getNumPartitions === 2)

    manualClock.advance(2000) // 2 secs
    assert(source.getOffset === Some(LongOffset(1)))
    val batch1 = source.getBatch(Some(LongOffset(0)), LongOffset(1))
    assert(batch1.count() === (2 * 10))
    assert(batch1.queryExecution.toRdd.getNumPartitions === 2)
    val allValues = batch1.as[Long].collect()
    assert(allValues.forall { x => x >= 1 && x <= 100 })
  }


  test("options") {
    val random = spark.readStream
      .format("random")
      .option("max", "1000")
      .option("rate", "10")
      .option("partitions", "2")
      .load()

    val counts = random.groupBy("value").count()

    val counter = new StreamingQueryListener {
      @volatile var numRowsProcessed: Long = 0L
      override def onQueryProgress(queryProgress: QueryProgress): Unit = {
        val status = queryProgress.queryStatus
        numRowsProcessed +=
          status.triggerStatus.asScala.getOrElse("numRows.input.total", "0").toLong

      }
      override def onQueryStarted(queryStarted: QueryStarted): Unit = {}
      override def onQueryTerminated(queryTerminated: QueryTerminated): Unit = {}

    }
    try {
      spark.streams.addListener(counter)
      val query = counts.writeStream
        .format("memory")
        .queryName("test")
        .outputMode("complete")
        .start()
      eventually(timeout(30 seconds)) {
        assert(counter.numRowsProcessed > 100)
      }
      query.stop()
    } finally {
      spark.streams.removeListener(counter)
    }
  }
}
