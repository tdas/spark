package org.apache.spark.sql.streaming.v2

import org.apache.spark.sql.streaming.{TimeBasedWindow, GlobalWindow, WindowSpec}
import org.apache.spark.sql.{DataFrame, SQLContext}

/** Abstraction representing a stream. Only pipelinable operators are present in the interface */
abstract class DataStream(@transient sqlContext: SQLContext) {

  def select(col: String, cols: String*): DataStream

  def filter(conditionExpr: String): DataStream

  def window(windowSpec: WindowSpec): DataFrame
}


object Examples {

  // Simple selection filtering
  ds.select("col1", "col2").filter("col3 < 50")

  // Window grouped countx
  ds.window(TimeBasedWindow.every(10).over(60)).groupBy("col3").agg("col1" -> "max")

  // Running grouped count [generate data at what interval?]
  ds.window(GlobalWindow.every(10)).groupBy("col3").agg("col1" -> "max")

  // Running global count [generate data at what interval?]
  ds.window(GlobalWindow.every(10)).agg("col1" -> "max")




  // ==== IGNORE BELOW ====

  def sqlContext: SQLContext = null

  def ds: DataStream = null
}
