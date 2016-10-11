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

import java.util.concurrent.atomic.AtomicLong

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.{Clock, SystemClock}

case class RandomStreamSource(
    sqlContext: SQLContext,
    maxValue: Long,
    ratePerSec: Long,
    numPartitions: Int,
    clock: Clock = new SystemClock) extends Source  {

  import sqlContext.implicits._

  private val currentOffset = new AtomicLong(0L)

  @volatile
  private var lastBatchTime: Long = -1L

  override val schema: StructType = RandomStreamSourceProvider.schema

  override def getOffset: Option[Offset] = {
    Some(LongOffset(currentOffset.getAndIncrement()))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    if (start.isEmpty) {
      assert(lastBatchTime < 0)
      lastBatchTime = clock.getTimeMillis()
      sqlContext.range(0, 0, 1, numPartitions)
    } else {
      assert(lastBatchTime >= 0)
      val max = maxValue
      val rate = ratePerSec
      val now = clock.getTimeMillis()
      val numValuesToGenerate = ((now - lastBatchTime) / 1000.0 * rate).toLong
      lastBatchTime = now
      sqlContext.range(0, numValuesToGenerate, 1, numPartitions).as[Long].mapPartitions { iter =>
        val random = new Random()
        iter.map { _ =>
          val r = random.nextLong
          ((r % max).toInt + max) % max + 1
        }
      }.toDF()
    }
  }

  override def stop(): Unit = {}

  override def toString(): String =
    s"Random[max=$maxValue,rate=$ratePerSec,partitions=$numPartitions]"
}

class RandomStreamSourceProvider extends StreamSourceProvider with DataSourceRegister {

  case class Params(maxValue: Long, ratePerSec: Long, numPartitions: Int)

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    parseParams(schema, parameters)
    ("random", RandomStreamSourceProvider.schema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val (max, rate, partitions) = Params.unapply(parseParams(schema, parameters)).get
    new RandomStreamSource(sqlContext, max, rate, partitions)
  }

  private def parseParams(
      schema: Option[StructType],
      parameters: Map[String, String]): Params = {
    if (schema.nonEmpty) {
      throw new IllegalArgumentException("Cannot specify schema for 'random' source")
    }
    val params = new CaseInsensitiveMap(parameters)
    val maxValue = params.get("max").map(_.toLong).getOrElse {
      throw new IllegalArgumentException("Must specify option 'max'")
    }
    val ratePerSec = params.get("rate").map(_.toLong).getOrElse {
      throw new IllegalArgumentException("Must specify option 'rate'")
    }
    val numPartitions = params.get("partitions").getOrElse("10").toInt
    Params(maxValue, ratePerSec, numPartitions)
  }

  override def shortName(): String = "random"
}

object RandomStreamSourceProvider {
  private[streaming] val schema = new StructType().add("value", LongType, nullable = false)
}
