package org.apache.spark.streaming.kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{BlockRDD, BlockRDDPartition}
import org.apache.spark.storage.BlockId

private[kinesis]
case class SequenceNumberRange(
    streamName: String, shardId: String, fromSeqNumber: String, toSeqNumber: String)

private[kinesis]
case class SequenceNumberRanges(ranges: Array[SequenceNumberRange]) {
  def isEmpty(): Boolean = ranges.isEmpty
  def nonEmpty(): Boolean = ranges.nonEmpty
  override def toString(): String = ranges.mkString("SequenceNumberRanges(", ", ", ")")
}

private[kinesis]
object SequenceNumberRanges {

  def apply(range: SequenceNumberRange): SequenceNumberRanges = {
    new SequenceNumberRanges(Array(range))
  }

  def apply(ranges: Seq[SequenceNumberRange]): SequenceNumberRanges = {
    new SequenceNumberRanges(ranges.toArray)
  }

  def empty: SequenceNumberRanges = {
    new SequenceNumberRanges(Array.empty)
  }
}

private[kinesis]
class KinesisBackedBlockRDDPartition(
    idx: Int,
    blockId: BlockId,
    val isBlockIdValid: Boolean,
    val seqNumberRanges: SequenceNumberRanges
  ) extends BlockRDDPartition(blockId, idx)

private[kinesis]
class KinesisBackedBlockRDD(
    sc: SparkContext,
    regionId: String,
    endpointUrl: String,
    @transient blockIds: Array[BlockId],
    @transient arrayOfseqNumberRanges: Array[SequenceNumberRanges],
    @transient isBlockIdValid: Array[Boolean] = Array.empty
) extends BlockRDD[Array[Byte]](sc, blockIds) {

  require(blockIds.length == arrayOfseqNumberRanges.length,
    "Number of blockIds is not equal to the number of sequence number ranges")

  override def isValid(): Boolean = true

  override def getPartitions: Array[Partition] = {
    Array.tabulate(blockIds.length) { i =>
      val isValid = if (isBlockIdValid.length == 0) true else isBlockIdValid(i)
      new KinesisBackedBlockRDDPartition(i, blockIds(i), isValid, arrayOfseqNumberRanges(i))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[KinesisBackedBlockRDDPartition]
    val blockId = partition.blockId

    def getBlockFromBlockManager(): Option[Iterator[Array[Byte]]] = {
      logDebug(s"Read partition data of $this from block manager, block $blockId")
      blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[Array[Byte]]])
    }

    def getBlockFromKinesis(): Iterator[Array[Byte]] = {
      val kinesisProxy = new KinesisProxy(
        new DefaultAWSCredentialsProviderChain(), endpointUrl, regionId)
      partition.seqNumberRanges.ranges.iterator.flatMap { range =>
        kinesisProxy.getSequenceNumberRange(
          range.streamName, range.shardId, range.fromSeqNumber, range.toSeqNumber)
      }
    }
    if (partition.isBlockIdValid) {
      getBlockFromBlockManager().getOrElse { getBlockFromKinesis() }
    } else {
      getBlockFromKinesis()
    }
  }
}
