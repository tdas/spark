package org.apache.spark.streaming.storage

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.storage.WriteAheadLogManager
import org.apache.spark.util.Utils
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.util.Clock
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo

private[streaming] sealed trait ReceivedBlockTrackerRecord

private[streaming] case class BlockAddition(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerRecord
private[streaming] case class BatchAllocations(time: Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerRecord
private[streaming] case class BatchCleanup(times: Seq[Time])
  extends ReceivedBlockTrackerRecord

case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
  def apply(streamId: Int) = streamIdToAllocatedBlocks(streamId)
}

private[streaming]
class ReceivedBlockTracker(
    conf: SparkConf, hadoopConf: Configuration, streamIds: Seq[Int], clock: Clock,
    checkpointDirOption: Option[String]) extends Logging {

  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]
  
  private val streamIdToUnallocatedBlockInfo = new mutable.HashMap[Int, ReceivedBlockQueue]
  private val timeToAllocatedBlockInfo = new mutable.HashMap[Time, AllocatedBlocks]

  private val logManagerRollingIntervalSecs = conf.getInt(
    "spark.streaming.receivedBlockTracker.writeAheadLog.rotationIntervalSecs", 60)
  private val logManagerOption = checkpointDirOption.map { checkpointDir =>
    new WriteAheadLogManager(
      ReceivedBlockTracker.checkpointDirToLogDir(checkpointDir),
      hadoopConf,
      rollingIntervalSecs = logManagerRollingIntervalSecs,
      callerName = "ReceivedBlockHandlerMaster",
      clock = clock
    )
  }

  // Recover block information from write ahead logs
  recoverFromWriteAheadLogs()

  /** Add received block */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = synchronized {
    try {
      writeToLog(BlockAddition(receivedBlockInfo))
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
      logDebug(s"Stream ${receivedBlockInfo.streamId} received block ${receivedBlockInfo.blockId}")
      true
    } catch {
      case e: Exception =>
        logError("Error adding block " + receivedBlockInfo, e)
        false
    }
  }

  /** Get blocks that have been added but not yet allocated to any batch */
  def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo] = synchronized {
    getReceivedBlockQueue(streamId).toSeq
  } 

  /** Get the blocks allocated to a batch, or allocate blocks to the batch and then get them */
  def getOrAllocateBlocksToBatch(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      if (!timeToAllocatedBlockInfo.contains(batchTime)) {
        allocateAllUnallocatedBlocksToBatch(batchTime)
      }
      timeToAllocatedBlockInfo(batchTime)(streamId)
    }
  }

  /** Check if any blocks are left to be allocated to batches */
  def hasUnallocatedReceivedBlocks(): Boolean = synchronized {
    !streamIdToUnallocatedBlockInfo.values.forall(_.isEmpty)
  }

  /** Clean up block information of old batches */
  def cleanupOldBatches(cleanupThreshTime: Time): Unit = synchronized {
    assert(cleanupThreshTime.milliseconds < clock.currentTime())
    val timesToCleanup = timeToAllocatedBlockInfo.keys.filter { _ < cleanupThreshTime }.toSeq
    logInfo("Deleting batches " + timesToCleanup)
    writeToLog(BatchCleanup(timesToCleanup))
    timeToAllocatedBlockInfo --= timesToCleanup
    logManagerOption.foreach(_.cleanupOldLogs(cleanupThreshTime.milliseconds))
    log
  }

  /** Stop the block tracker */
  def stop() {
    logManagerOption.foreach { _.stop() }
  }

  /** Allocate all unallocated blocks to the given batch */
  private def allocateAllUnallocatedBlocksToBatch(batchTime: Time): AllocatedBlocks = synchronized {
    val allocatedBlockInfos = AllocatedBlocks(streamIds.map { streamId =>
      (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
    }.toMap)
    writeToLog(BatchAllocations(batchTime, allocatedBlockInfos))
    timeToAllocatedBlockInfo(batchTime) = allocatedBlockInfos
    allocatedBlockInfos
  }

  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockInfo.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }

  /** Write an update to the tracker to the write ahead log */
  private def writeToLog(record: ReceivedBlockTrackerRecord) {
    logDebug("Writing to log " + record)
    logManagerOption.foreach { logManager =>
      logManager.writeToLog(ByteBuffer.wrap(Utils.serialize(record)))
    }
  }

  private def recoverFromWriteAheadLogs(): Unit = synchronized {
    logInfo("Recovering from checkpoint")
    val blockIdToblockInfo = new mutable.HashMap[StreamBlockId, ReceivedBlockInfo]

    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo) {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      println(s"Recovery: Inserting added block $receivedBlockInfo")
      blockIdToblockInfo.put(receivedBlockInfo.blockId, receivedBlockInfo)
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }

    def insertAllocatedBatch(time: Time, allocatedBlocks: AllocatedBlocks) {
      logTrace(s"Recovery: Inserting allocated batch for time $time to ${allocatedBlocks.streamIdToAllocatedBlocks}")
      println(s"Recovery: Inserting allocated batch for time $time to ${allocatedBlocks.streamIdToAllocatedBlocks}")
      streamIdToUnallocatedBlockInfo.values.foreach { _.clear() }
      timeToAllocatedBlockInfo.put(time, allocatedBlocks)
    }

    def cleanupBatches(batchTimes: Seq[Time]) {
      logTrace(s"Recovery: Cleaning up batches ${batchTimes}")
      println(s"Recovery: Cleaning up batches ${batchTimes}")
      timeToAllocatedBlockInfo --= batchTimes
    }

    logManagerOption.foreach { logManager =>
      logManager.readFromLog().foreach { byteBuffer =>
        logTrace("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerRecord](byteBuffer.array) match {
          case BlockAddition(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocations(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanup(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
    blockIdToblockInfo.clear()
  }
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}
