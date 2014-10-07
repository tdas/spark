package org.apache.spark.streaming.storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import WriteAheadLogBasedBlockHandler._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.{BlockManager, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.util.{Clock, SystemClock}
import org.apache.spark.util.Utils

private[streaming] sealed trait ReceivedBlock
private[streaming] case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) extends ReceivedBlock
private[streaming] case class IteratorBlock(iterator: Iterator[_]) extends ReceivedBlock
private[streaming] case class ByteBufferBlock(byteBuffer: ByteBuffer) extends ReceivedBlock

private[streaming] trait ReceivedBlockHandler {
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef]
  def clearOldBlocks(threshTime: Long)
}

private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel
  ) extends ReceivedBlockHandler {
  
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef] = {
    receivedBlock match {
      case ArrayBufferBlock(arrayBuffer) =>
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel, tellMaster = true)
      case IteratorBlock(iterator) =>
        blockManager.putIterator(blockId, iterator, storageLevel, tellMaster = true)
      case ByteBufferBlock(byteBuffer) =>
        blockManager.putBytes(blockId, byteBuffer, storageLevel, tellMaster = true)
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }
    None
  }

  def clearOldBlocks(threshTime: Long) {
    // this is not used as blocks inserted into the BlockManager are cleared by DStream's clearing
    // of BlockRDDs.
  }
}

private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
  ) extends ReceivedBlockHandler with Logging {

  private val blockStoreTimeout = conf.getInt(
    "spark.streaming.receiver.blockStoreTimeout", 30).seconds
  private val rollingInterval = conf.getInt(
    "spark.streaming.receiver.writeAheadLog.rollingInterval", 60)
  private val maxFailures = conf.getInt(
    "spark.streaming.receiver.writeAheadLog.maxFailures", 3)

  private val logManager = new WriteAheadLogManager(
    checkpointDirToLogDir(checkpointDir, streamId),
    hadoopConf, rollingInterval, maxFailures,
    threadPoolName = "WriteAheadLogBasedHandler.WriteAheadLogManager",
    clock = clock
  )

  implicit private val executionContext = ExecutionContext.fromExecutorService(
    Utils.newDaemonFixedThreadPool(1, "WriteAheadLogBasedBlockHandler"))
  
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef] = {
    val serializedBlock = receivedBlock match {
      case ArrayBufferBlock(arrayBuffer) =>
        blockManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        blockManager.dataSerialize(blockId, iterator)
      case ByteBufferBlock(byteBuffer) =>
        byteBuffer
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }

    val pushToBlockManagerFuture = Future {
      blockManager.putBytes(blockId, serializedBlock, storageLevel, tellMaster = true)
    }
    val pushToLogFuture = Future { logManager.writeToLog(serializedBlock) }
    val combinedFuture = for {
      _ <- pushToBlockManagerFuture
      fileSegment <- pushToLogFuture
    } yield fileSegment

    Some(Await.result(combinedFuture, blockStoreTimeout))
  }

  def clearOldBlocks(threshTime: Long) {
    logManager.clearOldLogs(threshTime)
  }

  def stop() {
    logManager.stop()
  }
}

private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}
