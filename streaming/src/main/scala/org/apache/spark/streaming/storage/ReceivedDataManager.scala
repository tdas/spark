package org.apache.spark.streaming.storage

import scala.language.postfixOps

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.{BlockManager, StorageLevel, StreamBlockId}

private[streaming] sealed trait ReceivedBlock
private[streaming] case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) extends ReceivedBlock
private[streaming] case class IteratorBlock(iterator: Iterator[_]) extends ReceivedBlock
private[streaming] case class ByteBufferBlock(byteBuffer: ByteBuffer) extends ReceivedBlock

private[streaming] trait ReceivedBlockHandler {
  def store(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef]
  def clear(threshTime: Long) { }
}

private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel
  ) extends ReceivedBlockHandler {
  
  def store(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef] = {
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
}

private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String
  ) extends ReceivedBlockHandler with Logging {

  private val logManager = new WriteAheadLogManager(
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString,
    conf, hadoopConf
  )

  private val blockStoreTimeout =
    conf.getInt("spark.streaming.receiver.blockStoreTimeout", 30) seconds

  implicit private val executionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
  
  def store(blockId: StreamBlockId, receivedBlock: ReceivedBlock): Option[AnyRef] = {
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
}
