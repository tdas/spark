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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.RangeStreamSource._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.{Clock, SystemClock}

case class RangeStreamSource(
    sqlContext: SQLContext,
    range: DataRange,
    strategy: GeneratingStrategy,
    numPartitions: Int,
    metadataPath: String,
    clock: Clock = new SystemClock) extends Source with Logging {

  private lazy val startTimeMs: Long = {
    val metadataLog = new HDFSMetadataLog[Long](sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val startTimeMs = clock.getTimeMillis()
      metadataLog.add(0, startTimeMs)
      logInfo(s"Start time: $startTimeMs")
      startTimeMs
    }
  }
  @volatile
  private var currentValue: Long = -1L

  override val schema: StructType = RangeStreamSource.schema

  override def getOffset: Option[Offset] = {
    startTimeMs
    if (currentValue < 0) {
      currentValue = range.start
    } else {
      val expectedCurrentValue = strategy match {
        case FixedRate(rate) =>
          ((clock.getTimeMillis() - startTimeMs) / 1000.0 * rate).toLong + range.start
        case FixedCount(count) =>
          currentValue + count
        case _ => throw new IllegalArgumentException(s"Unknown strategy $strategy")
      }
      currentValue = (range.end.toSeq :+ expectedCurrentValue).min
    }
    logDebug(s"GetOffset: $currentValue")
    Some(LongOffset(currentValue))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    startTimeMs
    if (currentValue < 0) currentValue = end.asInstanceOf[LongOffset].offset
    if (start.isEmpty) {
      sqlContext.range(0, 0, 1, numPartitions)
    } else {
      val from = start.get.asInstanceOf[LongOffset].offset
      val until = end.asInstanceOf[LongOffset].offset
      assert(from < until)
      logDebug(s"GetBatch: $from -> $until")
      sqlContext.range(from, until, 1, numPartitions)
    }
  }

  override def stop(): Unit = {}

  override def toString(): String =
    s"Range[strategy=$strategy,range=$range,partitions=$numPartitions]"
}

object RangeStreamSource {
  sealed trait GeneratingStrategy
  case class FixedRate(rate: Double) extends GeneratingStrategy
  case class FixedCount(count: Long) extends GeneratingStrategy

  case class DataRange(start: Long, end: Option[Long])

  val schema = new StructType().add("id", LongType, nullable = false)
}


class RangeStreamSourceProvider extends StreamSourceProvider with DataSourceRegister {

  case class SourceOptions(range: DataRange, strategy: GeneratingStrategy, numPartitions: Int)

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    parseParams(schema, parameters)
    ("range", RangeStreamSource.schema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val (range, strategy, partitions) = SourceOptions.unapply(parseParams(schema, parameters)).get
    new RangeStreamSource(sqlContext, range, strategy, partitions, metadataPath)
  }

  private def parseParams(
      schema: Option[StructType],
      parameters: Map[String, String]): SourceOptions = {
    if (schema.nonEmpty) {
      throw new IllegalArgumentException("Cannot specify schema for 'random' source")
    }
    val allowedStrategies = Seq("fixedrate", "fixedcount")
    val params = new CaseInsensitiveMap(parameters)

    val specifiedStrategies =
      params.filter { case (k, _) => allowedStrategies.contains(k) }.toSeq
    if (specifiedStrategies.isEmpty) {
      throw new IllegalArgumentException(
        "One of the following options must be specified for generator source: "
          + allowedStrategies.mkString(", ") + ". See the docs for more details.")
    } else if (specifiedStrategies.size > 1) {
      throw new IllegalArgumentException(
        "Only one of the following options can be specified for generator source: "
          + allowedStrategies.mkString(", ") + ". See the docs for more details.")
    }

    val strategy = params.find(x => allowedStrategies.contains(x._1)).get match {
      case ("fixedrate", value) =>
        FixedRate(value.trim.toDouble)
      case ("fixedcount", value) =>
        FixedCount(value.trim.toLong)
      case _ =>
        // Should never reach here as we are already matching on
        // matched strategy names
        throw new IllegalArgumentException("Unknown option")
    }

    val start = params.get("start").map(_.trim.toLong).getOrElse(0L)
    val endOption = params.get("end").map(_.trim.toLong)
    val range = DataRange(start, endOption)

    val numPartitions = params.get("partitions").getOrElse("10").toInt
    SourceOptions(range, strategy, numPartitions)
  }

  override def shortName(): String = "range"
}

