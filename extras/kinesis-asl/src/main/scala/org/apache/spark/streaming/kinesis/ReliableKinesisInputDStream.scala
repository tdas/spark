package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

class ReliableKinesisInputDStream(
    ssc: StreamingContext,
    streamName: String,
    endpointUrl: String,
    regionId: String,
    initialPositionInStream: InitialPositionInStream,
    checkpointAppName: String,
    checkpointInterval: Duration,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[Array[Byte]](ssc) {

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {

    val blockInfos =
      context.scheduler.receiverTracker.getBlocksOfBatch(validTime).get(id).getOrElse(Seq.empty)
    val blockStoreResults = blockInfos.map { _.blockStoreResult }
    val blockIds = blockStoreResults.map { _.blockId.asInstanceOf[BlockId] }.toArray

    val blockMetadata = blockInfos.map  { _.metadataOption }
    val blockRDD = if (blockMetadata.forall(_.nonEmpty)) {
      val seqNumRanges = blockMetadata.map { _.get.asInstanceOf[SequenceNumberRanges] }.toArray
      println(s"Creating KBBRDD for $validTime with seq number ranges = ${seqNumRanges.mkString(", ")} ")
      new KinesisBackedBlockRDD(context.sc, regionId, endpointUrl, blockIds, seqNumRanges)
    } else {
      logWarning("Kinesis sequence number information not present with block metadata, " +
        "may not be possible to recover from failures")
      new BlockRDD[Array[Byte]](context.sc, blockIds)
    }
    Some(blockRDD)
  }

  override def getReceiver(): Receiver[Array[Byte]] = {
    new ReliableKinesisReceiver(streamName, endpointUrl, checkpointAppName,
      checkpointInterval, initialPositionInStream, storageLevel)
  }
}
