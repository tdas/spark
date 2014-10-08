package org.apache.spark.streaming.storage

import java.io.File

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{ActorSystem, Props}
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{MapOutputTrackerMaster, SecurityManager, SparkConf}
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.storage._
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.util.AkkaUtils
import WriteAheadLogBasedBlockHandler._
import WriteAheadLogSuite._

class ReceivedBlockHandlerSuite extends FunSuite with BeforeAndAfter with Matchers {

  val conf = new SparkConf()
    .set("spark.authenticate", "false")
    .set("spark.kryoserializer.buffer.mb", "1")
    .set("spark.streaming.receiver.writeAheadLog.rollingInterval", "1")
  val hadoopConf = new Configuration()
  val storageLevel = StorageLevel.MEMORY_ONLY_SER
  val streamId = 1
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)
  val serializer = new KryoSerializer(conf)
  val manualClock = new ManualClock

  var actorSystem: ActorSystem = null
  var blockManagerMaster: BlockManagerMaster = null
  var blockManager: BlockManager = null
  var receivedBlockHandler: ReceivedBlockHandler = null
  var tempDirectory: File = null

  before {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      "test", "localhost", 0, conf = conf, securityManager = securityMgr)
    this.actorSystem = actorSystem

    conf.set("spark.driver.port", boundPort.toString)

    blockManagerMaster = new BlockManagerMaster(
      actorSystem.actorOf(Props(new BlockManagerMasterActor(true, conf, new LiveListenerBus))),
      conf, true)
    blockManager = new BlockManager("bm", actorSystem, blockManagerMaster, serializer, 100000,
      conf, mapOutputTracker, shuffleManager, new NioBlockTransferService(conf, securityMgr))
    tempDirectory = Files.createTempDir()
    manualClock.setTime(0)
  }

  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    actorSystem = null
    blockManagerMaster = null

    if (blockManager != null) {
      blockManager.stop()
      blockManager = null
    }

    if (tempDirectory != null && tempDirectory.exists()) {
      FileUtils.deleteDirectory(tempDirectory)
      tempDirectory = null
    }
  }

  test("WriteAheadLogBasedBlockHandler - store block") {
    createWriteAheadLogBasedBlockHandler()
    val (data, blockIds) = generateAndStoreData(receivedBlockHandler)
    receivedBlockHandler.asInstanceOf[WriteAheadLogBasedBlockHandler].stop()

    // Check whether blocks inserted in the block manager are correct
    val blockManagerData = blockIds.flatMap { blockId =>
      blockManager.getLocal(blockId).map { _.data }.getOrElse(Iterator.empty)
    }
    blockManagerData.toList shouldEqual data.toList

    // Check whether the blocks written to the write ahead log are correct
    val logFiles = getWriteAheadLogFiles()
    logFiles.size should be > 1

    val logData = logFiles.flatMap {
      file => new WriteAheadLogReader(file.toString, hadoopConf).toSeq
    }.flatMap { blockManager.dataDeserialize(StreamBlockId(streamId, 0), _ )}
    logData.toList shouldEqual data.toList
  }

  test("WriteAheadLogBasedBlockHandler - clear old blocks") {
    createWriteAheadLogBasedBlockHandler()
    generateAndStoreData(receivedBlockHandler)
    val preCleanupLogFiles = getWriteAheadLogFiles()
    preCleanupLogFiles.size should be > 1

    // this depends on the number of blocks inserted using generateAndStoreData()
    manualClock.currentTime() shouldEqual 5000L

    val cleanupThreshTime = 3000L
    receivedBlockHandler.clearOldBlocks(cleanupThreshTime)
    eventually(timeout(10000 millis), interval(10 millis)) {
      getWriteAheadLogFiles().size should be < preCleanupLogFiles.size
    }
  }

  def createWriteAheadLogBasedBlockHandler() {
    receivedBlockHandler = new WriteAheadLogBasedBlockHandler(blockManager, 1,
      storageLevel, conf, hadoopConf, tempDirectory.toString, manualClock)
  }

  def generateAndStoreData(
      receivedBlockHandler: ReceivedBlockHandler): (Seq[String], Seq[StreamBlockId]) = {
    val data = (1 to 100).map { _.toString }
    val blocks = data.grouped(10).map { _.toIterator }.toSeq
    val blockIds = (0 until blocks.size).map { i => StreamBlockId(streamId, i) }
    blocks.zip(blockIds).foreach { case (block, id) =>
      manualClock.addToTime(500)  // log rolling interval set to 1000 ms through SparkConf
      receivedBlockHandler.storeBlock(id, IteratorBlock(block))
    }
    (data, blockIds)
  }

  def getWriteAheadLogFiles(): Seq[File] = {
    getLogFilesInDirectory(
      new File(checkpointDirToLogDir(tempDirectory.toString, streamId)))
  }
}
