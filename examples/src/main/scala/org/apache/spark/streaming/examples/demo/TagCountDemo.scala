package org.apache.spark.streaming.examples.demo

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkContext._
import org.apache.spark.util.IntParam
import StreamingContext._
import TagCountDemoHelper._
import TwitterHelper._

object TagCountDemo {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TagCountDemo <master> <# streams> <checkpoint HDFS path>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), checkpointPath) = args

    // Create the StreamingContext
    val ssc = new StreamingContext(master, "TagCountDemo", Seconds(1),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint(checkpointPath)

    // Create the streams of tweets
    val tweets = ssc.union(
      (0 until numStreams).map(i => TwitterUtils.createStream(ssc, Some(authorizations(i))))
    )

    // Count the tags over a 1 minute window
    val tagCounts = tweets.flatMap(status => getTags(status))
                          .countByValueAndWindow(Minutes(1), Seconds(1))
    tagCounts.print()

    // Sort the tags by counts
    val sortedTags = tagCounts.map(_.swap).transform(_.sortByKey(false))
    sortedTags.foreachRDD(showTopTags(20) _)

    ssc.start()
    ssc.awaitTermination()
  }
}

