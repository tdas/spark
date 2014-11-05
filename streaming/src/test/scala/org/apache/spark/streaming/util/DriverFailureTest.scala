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

package org.apache.spark.streaming.util

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

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
    doTest()
  }

  /**
   * Actually perform the test on the context that has been setup using `setupContext`
   * and return any error message.
   */
  private def doTest(): Option[String] = {

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

/**
 * Companion object to [[org.apache.spark.streaming.util.DriverFailureTest]] containing
 * global state used while running a driver failure test.
 */
object DriverFailureTest {
  @transient @volatile var failed: Boolean = _
  @transient @volatile var firstFailureMessage: String = _

  /** Mark the currently running test as failed with the given error message */
  def fail(message: String) {
    if (!failed) {
      failed = true
      firstFailureMessage = message
    }
  }

  /** Reset the state */
  def reset() {
    failed = false
    firstFailureMessage = "NOT SET"
  }
}
