package org.apache.spark.sql.streaming.v1

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.{GlobalWindow, TimeBasedWindow, WindowSpec}
import org.apache.spark.sql.{GroupedData, Column, DataFrame, SQLContext}


/**
 * Abstraction representing a stream. Only pipelinable operators and groupBy are supported
 * among DataFrame operators; other operators throw UnsupportedOperationException
 */
abstract class StreamingDataFrame(@transient sqlContext: SQLContext)
  extends DataFrame(sqlContext, null.asInstanceOf[LogicalPlan]) {

  def window(windowSpec: WindowSpec): WindowedData

  override def count(): Long = {
    throw new UnsupportedOperationException(
      """Use sdf.window(GlobalWindow.every(Seconds(2))).count()") instead """)
  }

  override def agg(expr: Column, exprs: Column*): DataFrame = {
    throw new UnsupportedOperationException(
      """Use sdf.window(GlobalWindow.every(Seconds(2))).agg(...)") instead """)
  }
}


/** Abstraction representing a windowed data stream */
abstract class WindowedData {

  // V1 ops

  def count(): StreamingDataFrame

  def agg(aggExpr: (String, String), aggExprs: (String, String)*): StreamingDataFrame

  // V1+ pipelined ops

  def select(col: String, cols: String*): WindowedData

  def filter(conditionExpr: String): WindowedData
}

/**
 * Assume GroupedData has new method window(). In the code below,
 * it is emulated using GroupedDataToWindowedData helper implicit class
 */




object Examples {

  // Simple selection filtering
  sdf.select("col1", "col2").filter("col3 < 50")

  // Windowed grouped count
  sdf.groupBy("col3").window(TimeBasedWindow.every(10).over(60)).agg("col1" -> "max")

  // Running grouped count [generate data at what interval?]
  sdf.groupBy("col3").window(GlobalWindow.every(10)).agg("col1" -> "max")

  // Running global count [generate data at what interval?]
  sdf.window(GlobalWindow.every(10)).agg("col1" -> "max")


  // ==== Questions =====

  // What do these do as the API allows it?
  sdf.agg("col1" -> "max")
  sdf.groupBy("col3").agg("col1" -> "max")


  // ==== IGNORE BELOW ====

  def sqlContext: SQLContext = null

  def sdf: StreamingDataFrame = null

  // Helper class to enrich GroupedData to have window(). In real implementation,
  // GroupedData should have a new method window()
  implicit class GroupedDataToWindowedData(groupedData: GroupedData) {
    def window(windowSpec: WindowSpec): WindowedData = null
  }
}





