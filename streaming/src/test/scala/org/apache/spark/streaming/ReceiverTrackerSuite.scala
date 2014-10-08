package org.apache.spark.streaming

import java.io.File

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.pattern.ask
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{AddBlock, ReceivedBlockInfo, ReceivedBlockInfoCheckpointer, ReceiverTracker}
import org.apache.spark.streaming.storage.{FileSegment, WriteAheadLogReader}
import org.apache.spark.streaming.storage.WriteAheadLogSuite._
import org.apache.spark.util.Utils

class ReceiverTrackerSuite
  extends FunSuite with BeforeAndAfter with BeforeAndAfterAll with Matchers with Logging {

  val conf = new SparkConf().setMaster("local[2]").setAppName("ReceiverTrackerSuite")
  val sc = new SparkContext(conf)
  val akkaTimeout = 10 seconds

  var ssc: StreamingContext = null
  var receiverTracker: ReceiverTracker = null
  var checkpointDirectory: File = null

  before {
    checkpointDirectory = Files.createTempDir()
  }

  after {
    if (receiverTracker != null) {
      receiverTracker.stop()
      receiverTracker = null
    }
    if (ssc != null) {
      ssc.stop(stopSparkContext = false)
      ssc = null
    }
    if (checkpointDirectory != null && checkpointDirectory.exists()) {
      FileUtils.deleteDirectory(checkpointDirectory)
      checkpointDirectory = null
    }
  }

  override def afterAll() {
    sc.stop()
  }

  test("add blocks info and retrieve them") {
    createTracker(enableCheckpoint = false)
    val blockIds = (1 to 10).map { _ => math.abs(Random.nextInt()) }
    val futures = blockIds.map { id =>
      receiverTracker.actor.ask(AddBlock(createBlockInfo(id, 1)))(akkaTimeout).mapTo[Boolean]
    }
    Await.ready(Future.sequence(futures), akkaTimeout)

    val recordedBlockIds = receiverTracker.getReceivedBlockInfo(1)
    recordedBlockIds.map { _.blockId.uniqueId } shouldEqual blockIds
  }

  test("add block info with checkpoint enabled, and recover them") {
    createTracker(enableCheckpoint = true)
    val blockIds = (1 to 10).map { _ => math.abs(Random.nextInt()) }
    val futures = blockIds.map { id =>
      receiverTracker.actor.ask(AddBlock(createBlockInfo(id, 1)))(akkaTimeout).mapTo[Boolean]
    }
    Await.ready(Future.sequence(futures), akkaTimeout)
    receiverTracker.stop()

    val logFiles = getWriteAheadLogFiles()
    logFiles.size should be > 0

    val logData = logFiles.flatMap {
      file => new WriteAheadLogReader(file.toString, sc.hadoopConfiguration).toSeq
    }.map { byteBuffer =>
      Utils.deserialize[ReceivedBlockInfo](byteBuffer.array)
    }
    logData foreach { _.blockId.streamId shouldEqual 1 }
    logData.map { _.blockId.uniqueId }.toList shouldEqual blockIds

    createTracker(enableCheckpoint = true)
    val recoveredBlockIds = receiverTracker.getReceivedBlockInfo(1)
    recoveredBlockIds.map { _.blockId.uniqueId } shouldEqual blockIds
  }

  ignore("clean old block info") {
    // TODO (TD)
  }

  def createTracker(enableCheckpoint: Boolean) {
    ssc = new StreamingContext(sc, Seconds(1))
    if (enableCheckpoint) {
      ssc.checkpoint(checkpointDirectory.toString)
      assert(ssc.checkpointDir != null)
    }

    val receiver = new Receiver[String](StorageLevel.MEMORY_ONLY) {
      def onStart(): Unit = { }
      def onStop(): Unit = { }
    }
    ssc.receiverStream(receiver)
    receiverTracker = new ReceiverTracker(ssc, skipReceiverLaunch = true)
    receiverTracker.start()
    if (enableCheckpoint) {
      assert(receiverTracker.receivedBlockCheckpointerOption.isDefined)
    }

    // Wait for the receiver to be start before doing anything with it
    eventually(timeout(akkaTimeout), interval(10 milliseconds)) {
      receiverTracker.getReceivedBlockInfo(0) should not be None
    }
  }
  
  def createBlockInfo(blockUniqueId: Long, streamId: Int): ReceivedBlockInfo = {
    new ReceivedBlockInfo(streamId,
      StreamBlockId(streamId, blockUniqueId), 0, null, None)
  }

  def getWriteAheadLogFiles(): Seq[File] = {
    getLogFilesInDirectory(
      new File(ReceivedBlockInfoCheckpointer.checkpointDirToLogDir(checkpointDirectory.toString)))
  }
}
