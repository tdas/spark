package org.apache.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{ReceivedBlockInfo, ReceiverInfo, ReceiverTrackerListener, ReceiverTracker}
import org.apache.spark.util.{Utils, Clock}

/**
  * Created by tdas on 1/5/16.
  */
class ReceiverExecution(sparkContext: SparkContext, receivers: Seq[Receiver[_]]) {

  private val clock: Clock = {
    val clockClass = sparkContext.conf.get(
      "spark.streaming.clock", "org.apache.spark.util.SystemClock")
    try {
      Utils.classForName(clockClass).newInstance().asInstanceOf[Clock]
    } catch {
      case e: ClassNotFoundException if clockClass.startsWith("org.apache.spark.streaming") =>
        val newClockClass = clockClass.replace("org.apache.spark.streaming", "org.apache.spark")
        Utils.classForName(newClockClass).newInstance().asInstanceOf[Clock]
    }
  }

  private var receiverTracker = new ReceiverTracker(sparkContext, receivers, clock, true, None)

  def start(): Unit = synchronized {
    receiverTracker.start()
  }

  def stop(): Unit = synchronized {
    receiverTracker.stop(true)
  }

  def allocateBatch(): Unit = synchronized {
    clock.getTimeMillis()
    receiverTracker.allocateBlocksToBatch(Time(clock.getTimeMillis()))
  }

  def getLatestBatch(): Long = synchronized {
    receiverTracker.getAllBatches().map { _.milliseconds }.max
  }

  def getBlocksOfBatch(streamId: Int, batch: Long): Seq[ReceivedBlockInfo] = synchronized {
    receiverTracker.getBlocksOfBatchAndStream(Time(batch), streamId)
  }

  def getBlocksOfBatches(streamId: Int, start: Option[Long], end: Long): Seq[ReceivedBlockInfo] = {
    synchronized {
      val allBatches = receiverTracker.getAllBatches()
      val filteredBatches = if (start.isDefined) {
        allBatches.filter { batch => batch.milliseconds > start.get && batch.milliseconds <= end}
      } else {
        allBatches.filter { _.milliseconds <= end}
      }
      filteredBatches.sorted.flatMap { batch =>
        receiverTracker.getBlocksOfBatchAndStream(batch, streamId)
      }
    }
  }
}
