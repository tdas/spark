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

package org.apache.spark

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.concurrent._

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  /**
   * A buffer to store references to the CleanTaskWeakReference objects so
   * that they do not get GCed even before the actual to-be-cleaned-up objects
   * get GCed. These buffer needs to be cleaned explicitly when the
   * CleanTaskWeakReference objects are not needed any more.
   */
  private val referenceBuffer = new ArrayBuffer[CleanupTaskWeakReference]
    with SynchronizedBuffer[CleanupTaskWeakReference]

  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val concurrency = sc.conf.getInt("spark.cleaner.referenceTracking.concurrency", 10)
  private val concurrencySemaphore = new Semaphore(concurrency, true)

  private val listeners = new ArrayBuffer[CleanerListener] with SynchronizedBuffer[CleanerListener]

  private val executorService = Executors.newFixedThreadPool(2, new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setDaemon(true)
      t.setName("Spark ContextCleaner threadpool")
      t
    }
  })

  private implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener) {
    listeners += listener
  }

  /** Start the cleaner. */
  def start() {
    executorService.submit(new Runnable {
      def run() {
        keepCleaning()
      }
    })
  }

  /** Stop the cleaner. */
  def stop() {
    stopped = true
    executorService.shutdownNow()
  }

  /** Register a RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]) {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]) {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]) {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask) {
    referenceBuffer += new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue)
  }

  /** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.logUncaughtExceptions {
    while (!stopped) {
      try {
        val reference = referenceQueue.remove().asInstanceOf[CleanupTaskWeakReference]
        val task = reference.task
        logDebug("Got cleaning task " + task)
        referenceBuffer -= reference
        concurrencySemaphore.acquire()
        val future = doCleanupTask(task)
        logDebug("Submitted cleaning task " + task)
        future.onComplete { case t =>
          concurrencySemaphore.release()
          logDebug("Completed cleaning task " + task)
        }
      } catch {
        case ie: InterruptedException =>
          // Ignore interrupts, as while loop will ensure that cleaning keeps running
        case e: Exception =>
          logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform the cleanup task asynchronously. */
  private def doCleanupTask(cleanupTask: CleanupTask): Future[Unit] = {
    cleanupTask match {
      case CleanRDD(rddId) =>
        cleanupRDD(rddId)
      case CleanShuffle(shuffleId) =>
        cleanupShuffle(shuffleId)
      case CleanBroadcast(broadcastId) =>
        cleanupBroadcast(broadcastId)
    }
  }

  /** Perform RDD cleanup asynchronously. */
  def cleanupRDD(rddId: Int): Future[Unit] = {
    logDebug("Cleaning RDD " + rddId)
    val future = sc.unpersistRDDAsync(rddId)
    future.onSuccess { case _ =>
      listeners.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    }
    future.onFailure { case e =>
      logError("Error cleaning RDD " + rddId, e)
    }
    future
  }

  /** Perform shuffle cleanup asynchronously. */
  def cleanupShuffle(shuffleId: Int): Future[Unit] = {
    logDebug("Cleaning shuffle " + shuffleId)
    val future = blockManagerMaster.removeShuffleAsync(shuffleId).map { _ =>
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
    }
    future.onSuccess { case _ =>
      listeners.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    }
    future.onFailure { case e =>
      logError("Error cleaning shuffle " + shuffleId, e)
    }
    future
  }

  /** Perform broadcast cleanup asynchronously. */
  def cleanupBroadcast(broadcastId: Long): Future[Unit] = {
    logDebug("Cleaning broadcast " + broadcastId)
    val future = broadcastManager.unbroadcast(broadcastId, true)
    future.onSuccess { case _ =>
      listeners.foreach(_.broadcastCleaned(broadcastId))
      logInfo("Cleaned broadcast " + broadcastId)
    }
    future.onFailure { case e =>
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
    future
  }

  /** Perform RDD cleanup synchronously, used for testing. */
  def cleanupRDD(rddId: Int, timeout: Int): Unit = {
    Await.result(cleanupRDD(rddId), timeout millis)
  }

  /** Perform shuffle cleanup synchronously, used for testing. */
  def cleanupShuffle(shuffleId: Int, timeout: Int): Unit = {
    Await.result(cleanupShuffle(shuffleId), timeout millis)
  }

  /** Perform broadcast cleanup synchronously, used for testing. */
  def cleanupBroadcast(broadcastId: Long, timeout: Int): Unit = {
    Await.result(cleanupBroadcast(broadcastId), timeout millis)
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

/**
 * Listener class used for testing when any item has been cleaned by the Cleaner class.
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int)
  def shuffleCleaned(shuffleId: Int)
  def broadcastCleaned(broadcastId: Long)
}
