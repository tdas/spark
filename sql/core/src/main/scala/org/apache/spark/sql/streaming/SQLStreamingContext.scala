package org.apache.spark.sql.streaming

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.streaming.execution.StreamingPlanner
import org.apache.spark.sql.streaming.logical.LogicalDStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


case class Text(text: String)

class SQLStreamingContext(val sqlContext: SQLContext, val streamingContext: StreamingContext) {


  private[streaming] val planner = new StreamingPlanner(this)

  private[streaming] val outputOps = new ArrayBuffer[StreamFrameOutputOp]

  def socketTextStreamFrame(host: String, port: Int): StreamFrame = {
    createStreamFrame(streamingContext.socketTextStream(host, port).map(s => Text(s)) )
  }

  def createStreamFrame[A <: Product : TypeTag](dstream: DStream[A]): StreamFrame = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val outputTypes = schema.map(_.dataType)

    val rowDStream = dstream.mapPartitions { iterator =>
      val numColumns = outputTypes.length
      val mutableRow = new GenericMutableRow(numColumns)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
      iterator.map { r =>
        var i = 0
        while (i < numColumns) {
          mutableRow(i) = converters(i)(r.productElement(i))
          i += 1
        }

        mutableRow
      }
    }
    new StreamFrame(this,
      LogicalDStream(attributeSeq, rowDStream.asInstanceOf[DStream[InternalRow]]))
  }


  def start(): Unit = {
    outputOps.foreach { _.execute() }
    streamingContext.start()
  }

  def stop(): Unit = {
    streamingContext.stop()
  }
}
