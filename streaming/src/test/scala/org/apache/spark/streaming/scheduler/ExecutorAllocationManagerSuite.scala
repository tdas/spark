package org.apache.spark.streaming.scheduler

import scala.collection.mutable.ArrayBuffer

import org.scalatest.PrivateMethodTester
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => meq}

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.util.ManualClock
import org.apache.spark.{SparkConf, ExecutorAllocationClient, SparkFunSuite}
import org.scalatest.mock.MockitoSugar


class ExecutorAllocationManagerSuite extends SparkFunSuite
  with BeforeAndAfter with BeforeAndAfterAll with MockitoSugar with PrivateMethodTester {

  private var allocationClient: ExecutorAllocationClient = null
  private var receiverTracker: ReceiverTracker = null
  private var allocationManagers = new ArrayBuffer[ExecutorAllocationManager]
  private var clock: ManualClock = null

  before {
    allocationClient = mock[ExecutorAllocationClient]
    receiverTracker = mock[ReceiverTracker]
    clock = new ManualClock()
  }

  after {
    allocationManagers.foreach { _.stop() }
  }

  test("start") {
    val allocationManager = createManager()


  }

  test("requestExecutors") {
    val allocationManager = createManager()

    def assert(
        execIds: Seq[String],
        ratio: Double,
        expectedRequestedTotalExecs: Int): Unit = {
      reset(allocationClient)
      when(allocationClient.getExecutorIds()).thenReturn(execIds)
      requestExecutors(allocationManager, ratio)
      verify(allocationClient, times(1)).requestTotalExecutors(
        meq(expectedRequestedTotalExecs), meq(0), meq(Map.empty))
    }

    assert(Nil, 0, 1)
    assert(Nil, 1, 1)
    assert(Nil, 1.1, 1)
    assert(Nil, 1.6, 2)

    assert(Seq("1"), 1, 2)
    assert(Seq("1"), 1.1, 2)
    assert(Seq("1", "2"), 1.6, 4)
  }

  test("killExecutor") {
    val allocationManager = createManager()
    def assert(
        execIds: Seq[String],
      receiverExecIds: Map[Int, Option[String]],
      expectedKilledExec: Option[String]): Unit = {
      reset(allocationClient)
      reset(receiverTracker)
      when(allocationClient.getExecutorIds()).thenReturn(execIds)
      when(receiverTracker.getAllocatedExecutors()).thenReturn(receiverExecIds)
      killExecutor(allocationManager)
      if (expectedKilledExec.nonEmpty) {
        verify(allocationClient, times(1)).killExecutor(meq(expectedKilledExec.get))
      } else {
        verify(allocationClient, never()).killExecutor(null)
      }
    }

    assert(Nil, Map.empty, None)
    assert(Seq("1"), Map.empty, Some("1"))
    assert(Seq("1"), Map(1 -> Some("1")), None)
    assert(Seq("1", "2"), Map(1 -> Some("1")), Some("2"))
  }


  private def createManager(conf: SparkConf = new SparkConf): ExecutorAllocationManager = {
    val manager = new ExecutorAllocationManager(
      allocationClient, receiverTracker, conf, 1000, clock)
    allocationManagers += manager
    manager
  }

  private val _addBatchProcTime = PrivateMethod[Unit]('addBatchProcTime)
  private val _requestExecutors = PrivateMethod[Unit]('requestExecutors)
  private val _killExecutor = PrivateMethod[Unit]('killExecutor)

  private def addBatchProcTime(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _addBatchProcTime()
  }

  private def requestExecutors(manager: ExecutorAllocationManager, ratio: Double): Unit = {
    manager invokePrivate _requestExecutors(ratio)
  }

  private def killExecutor(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _killExecutor()
  }
}
