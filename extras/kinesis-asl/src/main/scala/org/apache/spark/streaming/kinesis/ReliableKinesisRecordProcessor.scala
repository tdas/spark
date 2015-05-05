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
package org.apache.spark.streaming.kinesis

import java.util.List

import scala.collection.JavaConversions.asScalaIterator
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.Logging

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

/**
 * Kinesis-specific implementation of the Kinesis Client Library (KCL) IRecordProcessor.
 * This implementation operates on the Array[Byte] from the KinesisReceiver.
 * The Kinesis Worker creates an instance of this KinesisRecordProcessor upon startup.
 *
 * @param receiver Kinesis receiver
 * @param workerId for logging purposes
 * @param checkpointState represents the checkpoint state including the next checkpoint time.
 *   It's injected here for mocking purposes.
 */
private[kinesis] class ReliableKinesisRecordProcessor(
    receiver: ReliableKinesisReceiver,
    workerId: String,
    checkpointState: KinesisCheckpointState) extends IRecordProcessor with Logging {

  /* shardId to be populated during initialize() */
  private var shardId: String = _

  private val blockGenerator = receiver.blockGenerator

  /**
   * The Kinesis Client Library calls this method during IRecordProcessor initialization.
   *
   * @param shardId assigned by the KCL to this particular RecordProcessor.
   */
  override def initialize(shardId: String) {
    this.shardId = shardId
    logDebug(s"Initialized: Worker $workerId initialized with shardId $shardId")
  }

  /**
   * This method is called by the KCL when a batch of records is pulled from the Kinesis stream.
   * This is the record-processing bridge between the KCL's IRecordProcessor.processRecords()
   * and Spark Streaming's Receiver.store().
   *
   * @param batch list of records from the Kinesis stream shard
   * @param checkpointer used to update Kinesis when this batch has been processed/stored 
   *   in the DStream
   */
  override def processRecords(batch: List[Record], checkpointer: IRecordProcessorCheckpointer) {
    if (!receiver.isStopped()) {
      try {

        logDebug(s"Received: Worker $workerId received ${batch.size} records for shardId $shardId")

        if (batch.size() > 0) {

          val dataIterator = batch.iterator().map { _.getData().array() }
          val metadata = SequenceNumberRange(receiver.streamName, shardId,
            batch.get(0).getSequenceNumber(), batch.get(batch.size() - 1).getSequenceNumber())
          blockGenerator.addMultipleDataWithCallback(dataIterator, metadata)
          logDebug(s"Stored: Worker $workerId stored ${batch.size} records for shardId $shardId")
        }

        /*
         * Checkpoint the sequence number of the last record successfully processed/stored 
         *   in the batch.
         * In this implementation, we're checkpointing after the given checkpointIntervalMillis.
         * Note that this logic requires that processRecords() be called AND that it's time to 
         *   checkpoint.  I point this out because there is no background thread running the 
         *   checkpointer.  Checkpointing is tested and trigger only when a new batch comes in.
         * If the worker is shutdown cleanly, checkpoint will happen (see shutdown() below).
         * However, if the worker dies unexpectedly, a checkpoint may not happen.
         * This could lead to records being processed more than once.
         */
        if (checkpointState.shouldCheckpoint()) {
          /* Perform the checkpoint */
          KinesisRecordProcessor.retryRandom(checkpointer.checkpoint(), 4, 100)

          /* Update the next checkpoint time */
          checkpointState.advanceCheckpoint()

          logDebug(s"Checkpoint: WorkerId $workerId completed checkpoint of ${batch.size}" +
            s" records for shardId $shardId")
          logDebug(s"Checkpoint: Next checkpoint is at " +
            s" ${checkpointState.checkpointClock.getTimeMillis()} for shardId $shardId")
        }
      } catch {
        case NonFatal(e) => {
          /*
           *  If there is a failure within the batch, the batch will not be checkpointed.
           *  This will potentially cause records since the last checkpoint to be processed
           *     more than once.
           */
          logError(s"Exception: WorkerId $workerId encountered and exception while storing " +
            " or checkpointing a batch for workerId $workerId and shardId $shardId.", e)

          /* Rethrow the exception to the Kinesis Worker that is managing this RecordProcessor.*/
          throw e
        }
      }
    } else {
      /* RecordProcessor has been stopped. */
      logInfo(s"Stopped:  The Spark KinesisReceiver has stopped for workerId $workerId" +
        s" and shardId $shardId.  No more records will be processed.")
    }
  }

  /**
   * Kinesis Client Library is shutting down this Worker for 1 of 2 reasons:
   * 1) the stream is resharding by splitting or merging adjacent shards 
   *     (ShutdownReason.TERMINATE)
   * 2) the failed or latent Worker has stopped sending heartbeats for whatever reason 
   *     (ShutdownReason.ZOMBIE)
   *
   * @param checkpointer used to perform a Kinesis checkpoint for ShutdownReason.TERMINATE
   * @param reason for shutdown (ShutdownReason.TERMINATE or ShutdownReason.ZOMBIE)
   */
  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) {
    logInfo(s"Shutdown:  Shutting down workerId $workerId with reason $reason")
    reason match {
      /*
       * TERMINATE Use Case.  Checkpoint.
       * Checkpoint to indicate that all records from the shard have been drained and processed.
       * It's now OK to read from the new shards that resulted from a resharding event.
       */
      case ShutdownReason.TERMINATE =>
        KinesisRecordProcessor.retryRandom(checkpointer.checkpoint(), 4, 100)

      /*
       * ZOMBIE Use Case.  NoOp.
       * No checkpoint because other workers may have taken over and already started processing
       *    the same records.
       * This may lead to records being processed more than once.
       */
      case ShutdownReason.ZOMBIE =>

      /* Unknown reason.  NoOp */
      case _ =>
    }
  }
}
