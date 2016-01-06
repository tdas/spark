package org.apache.spark.streaming

import org.apache.spark.Logging
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo


case class ReceiverSourceOffset(batchTime: Long, blockInfos: Seq[ReceivedBlockInfo]) extends Offset {
  override def isEmpty: Boolean = blockInfos.isEmpty

  override def >(other: Offset): Boolean = {
    other match {
      case other: ReceiverSourceOffset => this.batchTime > other.batchTime
      case _ =>
        throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
    }
  }

  override def <(other: Offset): Boolean = {
    other match {
      case other: ReceiverSourceOffset => this.batchTime < other.batchTime
      case _ =>
        throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
    }
  }
}

object ReceiverSourceOffset {
  def fromOffset(offset: Offset): ReceiverSourceOffset = {
    offset match {
      case o: ReceiverSourceOffset => o
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of" +
            s"${offset.getClass.getName.stripSuffix("$")} to $getClass")
    }
  }
}


trait ReceiverSource[T] extends Source with Logging {

  protected lazy val encoder = ExpressionEncoder[T]()

  @transient private var receiverExecution: ReceiverExecution = null

  def receivers: Seq[Receiver[T]]

  protected def getLatestBlocks(): (Long, Seq[ReceivedBlockInfo]) = {
    val batch = receiverExecution.getLatestBatch()
    val blockInfos = receivers.flatMap { receiver =>
      receiverExecution.getBlocksOfBatch(receiver.streamId, batch)
    }
    (batch, blockInfos)
  }

  protected def getBlocksByRange(start: Option[Offset], end: Offset): Seq[ReceivedBlockInfo] = {
    val startTime = start.map {
      ReceiverSourceOffset.fromOffset(_).batchTime
    }
    val endTime = ReceiverSourceOffset.fromOffset(end).batchTime
    receivers.flatMap { receiver =>
      receiverExecution.getBlocksOfBatches(receiver.streamId, startTime, endTime)
    }
  }

  override def schema: StructType = encoder.schema

  override def offset: Offset = {
    assert(receiverExecution != null,
      "Cannot get offset of receiver until ReceiverExecution is set")
    val (batchTime, blockInfos) = getLatestBlocks()
    ReceiverSourceOffset(batchTime, blockInfos)
  }

  override def getSlice(sqlContext: SQLContext, start: Option[Offset], end: Offset): RDD[InternalRow] = {

    val blockInfos = getBlocksByRange(start, end)

    val blockRDD = if (blockInfos.nonEmpty) {
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray

      // Are WAL record handles present with all the blocks
      val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty }

      if (areWALRecordHandlesPresent) {
        // If all the blocks have WAL record handle, then create a WALBackedBlockRDD
        val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
        val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
        new WriteAheadLogBackedBlockRDD[T](
          sqlContext.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
      } else {
        // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
        // others then that is unexpected and log a warning accordingly.
        if (blockInfos.find(_.walRecordHandleOption.nonEmpty).nonEmpty) {
          logWarning("Some blocks do not have Write Ahead Log information; this is unexpected")
        }
        val validBlockIds = blockIds.filter { id =>
          sqlContext.sparkContext.env.blockManager.master.contains(id)
        }
        if (validBlockIds.size != blockIds.size) {
          logWarning("Some blocks could not be recovered as they were not found in memory. " +
            "To prevent such data loss, enabled Write Ahead Log (see programming guide " +
            "for more details.")
        }
        new BlockRDD[T](sqlContext.sparkContext, validBlockIds)
      }
    } else {
      new BlockRDD[T](sqlContext.sparkContext, Array.empty)
    }
    val encodingFunc = encoder.toRow _
    blockRDD.map { encodingFunc }
  }

  /** For testing. */
  override def restart(): Source = ???

  private[spark] def setReceiverExecution(rcvrExec: ReceiverExecution): Unit = {
    receiverExecution = rcvrExec
  }
}


object ReceiverSource {
  def unapply(source: Source): Option[ReceiverSource] = source match {
    case rs: ReceiverSource => Some(rs)
    case _ => None
  }
}