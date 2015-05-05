package org.apache.spark.streaming.kinesis

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

class KinesisStreamSuite extends FunSuite with Eventually with BeforeAndAfter {
  private var ssc: StreamingContext = _

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
  }

  if (KinesisTestUtils.enableTests) {

    test("basic operation") {
      val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
      ssc = new StreamingContext(sparkConf, Milliseconds(1000))
      val testData = 1 to 10
      val collected = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int]
      val stream = KinesisUtils.createStream(ssc,
        KinesisTestUtils.streamName, KinesisTestUtils.endPointUrl, KinesisTestUtils.regionId,
        InitialPositionInStream.LATEST, "test", Seconds(10), StorageLevel.MEMORY_ONLY)

      stream.map { bytes => new String(bytes).toInt }.foreachRDD { rdd =>
        collected ++= rdd.collect()
        println("Collected = " + rdd.collect().toSeq.mkString("\n"))
      }
      ssc.start()

      eventually(timeout(600 seconds), interval(1 second)) {
        KinesisTestUtils.pushData(testData)
        assert(collected === testData.toSet, "\nData received does not match data sent")
      }
    }

    test("recovery") {
      val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
      val checkpointDir = Utils.createTempDir().getAbsolutePath

      ssc = new StreamingContext(sparkConf, Milliseconds(1000))
      ssc.checkpoint(checkpointDir)

      val stream = KinesisUtils.createStream(ssc,
        KinesisTestUtils.streamName, KinesisTestUtils.endPointUrl, KinesisTestUtils.regionId,
        InitialPositionInStream.LATEST, "test", Seconds(10), StorageLevel.MEMORY_ONLY)

      val collected = new mutable.HashMap[Time, Seq[Int]] with mutable.SynchronizedMap[Time, Seq[Int]]
      val mappedStream = stream.map { bytes => new String(bytes).toInt }
      val windowedStream = mappedStream.window(Seconds(60))
      windowedStream.foreachRDD((rdd: RDD[Int], time: Time) => {
        collected(time) = rdd.collect().toSeq
      })

      ssc.remember(Seconds(600)) // remember everything
      ssc.start()

      def numBatchesWithData: Int = collected.count(_._2.nonEmpty)

      def isCheckpointPresent: Boolean = Checkpoint.getCheckpointFiles(checkpointDir).nonEmpty

      eventually(timeout(60 seconds), interval(1 second)) {
        KinesisTestUtils.pushData(1 to 5)
        assert(isCheckpointPresent && numBatchesWithData > 10)
      }
      ssc.stop()
      println("Restarting")
      ssc = new StreamingContext(checkpointDir)
      ssc.start()

      val recoveredWindowedStream =
        ssc.graph.getOutputStreams().head.dependencies.head.asInstanceOf[DStream[Int]]
      collected.keySet.foreach { time =>
        assert(recoveredWindowedStream.getOrCompute(time).get.collect().toSeq === collected(time))
      }
      ssc.stop()
    }
  }
}