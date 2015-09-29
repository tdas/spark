package org.apache.spark.sql.streaming

import org.apache.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

object StreamFrameExample {
  case class Text(text: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingPlan")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, spark.streaming.Seconds(1))
    val sql = new SQLContext(sc)

    val sqlStreamingContext = new SQLStreamingContext(sql, ssc)
    val streamFrame = sqlStreamingContext.socketTextStreamFrame("localhost", 9999)

    val projectedFrame = streamFrame.selectExpr("text", "1 as age")
    val filteredFrame = projectedFrame.filter("""text != "hello" """)
    projectedFrame.print()

    sqlStreamingContext.start()
    ssc.awaitTermination()
  }
}
