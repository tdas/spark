package org.apache.spark.streaming.examples.demo

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.util.IntParam
import StreamingContext._
import TwitterDemoHelper._

object TwitterDemo {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TwitterDemo <master> <# streams> <checkpoint HDFS path>")
      System.exit(1)
    }

    val Array(master, IntParam(numStreams), checkpointPath) = args

    // Create the StreamingContext
    val ssc = new StreamingContext(master, "TwitterDemo", Seconds(1),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint(checkpointPath)

    // Create the streams of tweets
    val tweets = ssc.union(
      authorizations(numStreams).map(oauth => ssc.twitterStream(Some(oauth)))
    )

    // Count the tags over a 1 minute window
    val tagCounts = tweets.flatMap(status => getTags(status))
                          .countByValueAndWindow(Minutes(1), Seconds(1))
    tagCounts.print()

    // Sort the tags by counts
    val sortedTags = tagCounts.map(_.swap).transform(_.sortByKey(false))
    sortedTags.foreach(showTopTags(20) _)

    ssc.start()
  }
}

