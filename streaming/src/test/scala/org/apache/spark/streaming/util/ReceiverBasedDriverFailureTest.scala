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

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of the [[org.apache.spark.streaming.util.DriverFailureTest]] that uses
 * a receiver as the data source and test whether all the received data gets processed with
 * the given operation despite recurring driver failures.
 */
class ReceiverBasedDriverFailureTest[T](
    @transient batchDurationMillis: Int,
    @transient numBatchesToRun: Int,
    @transient operation: DStream[String] => DStream[T],
    @transient outputVerifyingFunction: (Time, Seq[T]) => Unit,
    @transient testDirectory: String)
  extends DriverFailureTest(testDirectory, batchDurationMillis, numBatchesToRun) {

  @transient val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("ReceiverBasedDriverFailureTest")
    .set("spark.streaming.receiver.writeAheadLog.enable", "true") // enable write ahead log
    .set("spark.streaming.receiver.writeAheadLog.rotationIntervalSecs", "10")
     // rotate logs to test cleanup

  override def setupContext(checkpointDirector: String): StreamingContext = {

    val context = StreamingContext.getOrCreate(checkpointDirector, () => {
      val newSsc = new StreamingContext(conf, Milliseconds(batchDurationMillis))
      val inputStream = newSsc.receiverStream[String](new DriverFailureTestReceiver)
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


/**
 * Implementation of [[org.apache.spark.streaming.receiver.Receiver]] that is used by
 * [[org.apache.spark.streaming.util.ReceiverBasedDriverFailureTest]] for failure recovery under
 * driver failures.
 */
class DriverFailureTestReceiver
  extends Receiver[String](StorageLevel.MEMORY_ONLY_SER) with Logging {

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


/**
 * Companion object of [[org.apache.spark.streaming.util.DriverFailureTestReceiver]]
 * containing global state used while running a driver failure test.
 */
object DriverFailureTestReceiver {
  val maxRecordsPerBlock = 1000L
  private val currentKey = new AtomicInteger()
  private val counter = new AtomicInteger()

  counter.set(1)
  currentKey.set(1)

  def getNextBlock(): ArrayBuffer[String] = {
    val count = counter.get()
    new ArrayBuffer ++= (1 to count).map {
      _ => "word%03d".format(currentKey.get())
    }
  }

  def commitBlock() {
    println(s"Stored ${counter.get()} copies of word${currentKey.get}")
    if (counter.incrementAndGet() > maxRecordsPerBlock) {
      currentKey.incrementAndGet()
      counter.set(1)
    }
  }
}

