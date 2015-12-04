package org.apache.spark.streaming.scheduler

import scala.util.Random

import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.Clock
import org.apache.spark.{ExecutorAllocationClient, Logging, SparkConf}

class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    receiverTracker: ReceiverTracker,
    conf: SparkConf,
    batchDurationMs: Long,
    clock: Clock)
  extends StreamingListener with Logging {

  private val scalingIntervalSecs = conf.getTimeAsSeconds(
    "spark.streaming.dynamicAllocation.scalingInterval", "1min")
  private val scalingUpRatio = conf.getDouble(
    "spark.streaming.dynamicAllocation.scalingUpRatio", 0.9)
  private val scalingDownRatio = conf.getDouble(
    "spark.streaming.dynamicAllocation.scalingDownRatio", 0.4)

  @volatile private var batchProcTimeSum = 0L
  @volatile private var batchProcTimeCount = 0

  private val timer =  new RecurringTimer(clock, scalingIntervalSecs * 1000,
    _ => manageAllocation(), "streaming-executor-allocation-manager")

  def start(): Unit = {
    timer.start()
    logInfo("ExecutorAllocationManager started")
  }

  def stop(): Unit = {
    timer.stop(interruptTimer = true)
    logInfo("ExecutorAllocationManager stopped")
  }

  private def manageAllocation(): Unit = synchronized {
    logInfo("Managing executor allocation")
    if (batchProcTimeCount > 0) {
      val averageBatchProcTime = batchProcTimeSum / batchProcTimeCount
      val ratio = averageBatchProcTime.toDouble / batchDurationMs
      if (ratio > scalingUpRatio) {
        requestExecutors(ratio)
      } else if (ratio < scalingDownRatio) {
        killExecutor()
      }
    }
    batchProcTimeSum = 0
    batchProcTimeCount = 0
  }

  private def requestExecutors(ratio: Double): Unit = {
    val allExecIds = client.getExecutorIds()
    val numExecsToRequest = math.max(math.round(ratio).toInt, 1)
    client.requestTotalExecutors(allExecIds.size + numExecsToRequest, 0, Map.empty)
  }

  private def killExecutor(): Unit = {
    val allExecIds = client.getExecutorIds()
    if (allExecIds.nonEmpty) {
      val execIdsWithReceivers = receiverTracker.getAllocatedExecutors().values.flatten.toSeq
      logInfo(s"Removable executors (${execIdsWithReceivers.size}): ${execIdsWithReceivers}")

      val removableExecIds = allExecIds.diff(execIdsWithReceivers)
      logInfo(s"Removable executors (${removableExecIds.size}): ${removableExecIds}")

      val execIdToRemove = removableExecIds(Random.nextInt(removableExecIds.size))
      client.killExecutor(execIdToRemove)
      logInfo(s"Requested to kill executor $execIdToRemove")
    } else {
      logInfo("No executors to kill")
    }
  }

  private def addBatchProcTime(timeMs: Long): Unit = synchronized {
    batchProcTimeSum += timeMs
    batchProcTimeCount += 1
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    batchCompleted.batchInfo.processingDelay.foreach(addBatchProcTime)
  }
}
