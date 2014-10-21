/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{HashPartitioner, Logging, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.Utils
import scala.collection.mutable.ArrayBuffer

/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class DriverFailureSuite extends TestSuiteBase with Logging {

  var directory = "FailureSuite"
  val numBatches = 30

  override def batchDuration = Milliseconds(1000)

  override def useManualClock = false

  override def beforeFunction() {
    super.beforeFunction()
    Utils.deleteRecursively(new File(directory))
  }

  override def afterFunction() {
    super.afterFunction()
    Utils.deleteRecursively(new File(directory))
  }
/*
  test("multiple failures with map") {
    MasterFailureTest.testMap(directory, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(directory, numBatches, batchDuration)
  }
*/
  test("multiple failures with receiver and updateStateByKey") {


    val operation = (st: DStream[String]) => {

      val mapPartitionFunc = (iterator: Iterator[String]) => {
        Iterator(iterator.flatMap(_.split(" ")).map(_ -> 1L).reduce((x, y) => (x._1, x._2 + y._2)))
      }

      val updateFunc = (iterator: Iterator[(String, Seq[Long], Option[Seq[Long]])]) => {
        iterator.map { case (key, values, state) =>
          val combined = (state.getOrElse(Seq.empty) ++ values).sorted
          if (state.isEmpty || state.get.max != DriverFailureTestReceiver.maxRecordsPerBlock) {
            val oldState = s"[${ state.map { _.max }.getOrElse(-1) }, ${state.map { _.distinct.sum }.getOrElse(0)}]"
            val newState = s"[${combined.max}, ${combined.distinct.sum}]"
            println(s"Updated state for $key: state = $oldState, new values = $values, new state = $newState")
          }
          (key, combined)
        }
      }

      st.mapPartitions(mapPartitionFunc)
        .updateStateByKey[Seq[Long]](updateFunc, new HashPartitioner(2), rememberPartitioner = false)
        .checkpoint(batchDuration * 5)
    }

    val maxValue = DriverFailureTestReceiver.maxRecordsPerBlock
    val expectedValues = (1L to maxValue).toSet

    val verify = (time: Time, output: Seq[(String, Seq[Long])]) => {
      val outputStr = output.map { x => (x._1, x._2.distinct.sum) }.sortBy(_._1).mkString(", ")
      println(s"State at $time: $outputStr")

      val incompletelyReceivedWords = output.filter { _._2.max < maxValue }
      if (incompletelyReceivedWords.size > 1) {
        val debugStr = incompletelyReceivedWords.map { x =>
          s"""${x._1}: ${x._2.mkString(",")}, sum = ${x._2.distinct.sum}"""
        }.mkString("\n")
        throw new Exception(s"Incorrect processing of input, all input not processed:\n$debugStr\n")
      }

      output.foreach { case (key, values) =>
        if (!values.forall(expectedValues.contains)) {
          val sum = values.distinct.sum
          val debugStr = values.zip(1L to values.size).map {
            x => if (x._1 == x._2) x._1 else s"[${x._2}]"
          }.mkString(",") + s", sum = $sum"
          throw new Exception(s"Incorrect sequence of values in output:\n$debugStr\n")
        }
      }
    }

    val driverTest = new ReceiverBasedDriverFailureTest[(String, Seq[Long])](
      "./driver-test/", 200, 50, operation, verify)
    driverTest.testAndGetError().map { errorMessage =>
      fail(errorMessage)
    }
  }
}


abstract class DriverFailureTest(
    testDirectory: String,
    batchDurationMillis: Int,
    numBatchesToRun: Int
  ) extends Logging {

  @transient private val checkpointDir = createCheckpointDir()
  @transient private val timeoutMillis = batchDurationMillis * numBatchesToRun * 4

  @transient @volatile private var killed = false
  @transient @volatile private var killCount = 0
  @transient @volatile private var lastBatchCompleted = 0L
  @transient @volatile private var batchesCompleted = 0
  @transient @volatile private var ssc: StreamingContext = null

  protected def setupContext(checkpointDirector: String): StreamingContext

  //----------------------------------------

  /**
   * Run the test and return an option string containing error message.
   * @return None is test succeeded, or Some(errorMessage) if test failed
   */
  def testAndGetError(): Option[String] = {
    DriverFailureTest.reset()
    ssc = setupContext(checkpointDir.toString)
    run()
  }

  private def run(): Option[String] = {

    val runStartTime = System.currentTimeMillis
    var killingThread: Thread = null

    def allBatchesCompleted = batchesCompleted >= numBatchesToRun
    def timedOut = (System.currentTimeMillis - runStartTime) > timeoutMillis
    def failed = DriverFailureTest.failed

    while(!failed && !allBatchesCompleted && !timedOut) {
      // Start the thread to kill the streaming after some time
      killed = false
      try {
        ssc.addStreamingListener(new BatchCompletionListener)
        ssc.start()

        killingThread = new KillingThread(ssc, batchDurationMillis * 10)
        killingThread.start()

        while (!failed && !killed && !allBatchesCompleted && !timedOut) {
          ssc.awaitTermination(1)
        }
      } catch {
        case e: Exception =>
          logError("Error running streaming context", e)
          DriverFailureTest.fail("Error running streaming context: " + e)
      }

      logInfo(s"Failed = $failed")
      logInfo(s"Killed = $killed")
      logInfo(s"All batches completed = $allBatchesCompleted")
      logInfo(s"Timed out = $timedOut")

      if (killingThread.isAlive) {
        killingThread.interrupt()
        ssc.stop()
      }

      if (!timedOut) {
        val sleepTime = Random.nextInt(batchDurationMillis * 10)
        logInfo(
          "\n-------------------------------------------\n" +
            "   Restarting stream computation in " + sleepTime + " ms   " +
            "\n-------------------------------------------\n"
        )
        Thread.sleep(sleepTime)

        // Recreate the streaming context from checkpoint
        System.clearProperty("spark.driver.port")
        ssc = StreamingContext.getOrCreate(checkpointDir.toString, () => {
          throw new Exception("Trying to create new context when it " +
            "should be reading from checkpoint file")
        })
        println("Restarted")
      }
    }

    if (failed) {
      Some(s"Failed with message: ${DriverFailureTest.firstFailureMessage}")
    } else if (timedOut) {
      Some(s"Timed out after $batchesCompleted/$numBatchesToRun batches, and " +
        s"${System.currentTimeMillis} ms (time out = $timeoutMillis ms)")
    } else if (allBatchesCompleted) {
      None
    } else {
      throw new Exception("Unexpected end of test")
    }
  }

  private def createCheckpointDir(): Path = {
    // Create the directories for this test
    val uuid = UUID.randomUUID().toString
    val rootDir = new Path(testDirectory, uuid)
    val fs = rootDir.getFileSystem(new Configuration())
    val dir = new Path(rootDir, "checkpoint")
    fs.mkdirs(dir)
    dir
  }
  
  class BatchCompletionListener extends StreamingListener {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      if (batchCompleted.batchInfo.batchTime.milliseconds > lastBatchCompleted) {
        batchesCompleted += 1
        lastBatchCompleted = batchCompleted.batchInfo.batchTime.milliseconds
      }
    }    
  }

  class KillingThread(ssc: StreamingContext, maxKillWaitTime: Long) extends Thread with Logging {
    override def run() {
      try {
        // If it is the first killing, then allow the first checkpoint to be created
        var minKillWaitTime = if (killCount == 0) 5000 else 2000
        val killWaitTime = minKillWaitTime + math.abs(Random.nextLong % maxKillWaitTime)
        logInfo("Kill wait time = " + killWaitTime)
        Thread.sleep(killWaitTime)
        logInfo(
          "\n---------------------------------------\n" +
            "Killing streaming context after " + killWaitTime + " ms" +
            "\n---------------------------------------\n"
        )
        ssc.stop()
        killed = true
        killCount += 1
        println("Killed")
        logInfo("Killing thread finished normally")
      } catch {
        case ie: InterruptedException => logInfo("Killing thread interrupted")
        case e: Exception => logWarning("Exception in killing thread", e)
      }

    }
  }
}

object DriverFailureTest {
  @transient @volatile var failed: Boolean = _
  @transient @volatile var firstFailureMessage: String = _

  def fail(message: String) {
    if (!failed) {
      failed = true
      firstFailureMessage = message
    }
  }

  def reset() {
    failed = false
    firstFailureMessage = "NOT SET"
  }
}

class ReceiverBasedDriverFailureTest[T](
    @transient testDirectory: String,
    @transient batchDurationMillis: Int,
    @transient numBatchesToRun: Int,
    @transient operation: DStream[String] => DStream[T],
    outputVerifyingFunction: (Time, Seq[T]) => Unit
  ) extends DriverFailureTest(
    testDirectory, batchDurationMillis, numBatchesToRun
  ) {

  @transient val conf = new SparkConf()
  conf.setMaster("local[4]")
      .setAppName("ReceiverBasedDriverFailureTest")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")  // enable write ahead log
      .set("spark.streaming.receiver.writeAheadLog.rotationIntervalSecs", "10")  // rotate logs to test cleanup

  override def setupContext(checkpointDirector: String): StreamingContext = {

    val context = StreamingContext.getOrCreate(checkpointDirector, () => {
      val newSsc = new StreamingContext(conf, Milliseconds(batchDurationMillis))
      val inputStream = newSsc.receiverStream[String](new DriverFailureTestReceiver)
      /*inputStream.mapPartitions(iter => {
        val sum = iter.map { _.split(" ").size }.fold(0)(_ + _)
        Iterator(sum)
      }).foreachRDD ((rdd: RDD[Int], time: Time) => {
        try {
          val collected =  rdd.collect().sorted
          println(s"# in partitions at $time = ${collected.mkString(", ")}")
        } catch {
          case ie: InterruptedException =>
          // ignore
          case e: Exception =>
            DriverFailureTest.fail(e.toString)
        }

      })*/
      val operatedStream = operation(inputStream)

      val verify = outputVerifyingFunction
      operatedStream.foreachRDD((rdd: RDD[T], time: Time) => {
        try {
          val collected = rdd.collect()
          verify(time, collected)
        } catch {
          case ie: InterruptedException =>
            // ignore
          case e: Exception =>
            DriverFailureTest.fail(e.toString)
        }
      })
      newSsc.checkpoint(checkpointDirector)
      newSsc
    })
    context
  }
}



class DriverFailureTestReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY_SER) with Logging {

  import DriverFailureTestReceiver._
  @volatile var thread: Thread = null

  class ReceivingThread extends Thread() {
    override def run() {
      while (!isStopped() && !isInterrupted()) {
        try {
          val block = getNextBlock()
          store(block)
          commitBlock()
          Thread.sleep(10)
        } catch {
          case ie: InterruptedException =>
          case e: Exception =>
            DriverFailureTestReceiver.this.stop("Error in receiving thread", e)
        }
      }
    }
  }

  def onStart() {
    if (thread == null) {
      thread = new ReceivingThread()
      thread.start()
    } else {
      logError("Error starting receiver, previous receiver thread not stopped yet.")
    }
  }

  def onStop() {
    if (thread != null) {
      thread.interrupt()
      thread = null
    }
  }
}

object DriverFailureTestReceiver {
  val maxRecordsPerBlock = 1000L
  private val currentKey = new AtomicInteger()
  private val counter = new AtomicInteger()

  counter.set(1)
  currentKey.set(1)

  def getNextBlock(): ArrayBuffer[String] = {
    val count = counter.get()
    new ArrayBuffer ++= (1 to count).map { _ => "word%03d".format(currentKey.get()) }
  }

  def commitBlock() {
    println(s"Stored ${counter.get()} copies of word${currentKey.get}")
    if (counter.incrementAndGet() > maxRecordsPerBlock) {
      currentKey.incrementAndGet()
      counter.set(1)
    }
  }
}

