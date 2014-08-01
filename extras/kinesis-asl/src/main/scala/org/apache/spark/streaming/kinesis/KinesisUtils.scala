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

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream


/**
 * Facade to create the Scala-based or Java-based streams.
 * Also, contains a reusable utility methods.
 * :: Experimental ::
 */
@Experimental
object KinesisUtils extends Logging {
  /**
   * Create an InputDStream that pulls messages from a Kinesis stream.
   *
   * @param ssc      StreamingContext object
   * @param appName  Kinesis application name. Kinesis Apps are mapped to Kinesis Streams
   *                 by the Kinesis Client Library.  If you change the App name or Stream name,
   *                 the KCL will throw errors.
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param checkpointIntervalMillis Checkpoint interval in milliseconds for Kinesis
   *                                 checkpointing (not Spark checkpointing). See the
   *                                 Kinesis Spark Streaming documentation for more
   *                                 details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @return ReceiverInputDStream[Array[Byte]]
   */
  def createStream(
      ssc: StreamingContext,
      appName: String,
      streamName: String,
      endpointUrl: String,
      checkpointIntervalMillis: Long,
      initialPositionInStream: InitialPositionInStream,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(new KinesisReceiver(appName, streamName, endpointUrl, checkpointIntervalMillis,
        initialPositionInStream, storageLevel))
  }

  /**
   * Create a Java-friendly InputDStream that pulls messages from a Kinesis stream.
   *
   * @param jssc      JavaStreamingContext object
   * @param appName  Kinesis application name. Kinesis Apps are mapped to Kinesis Streams
   *                 by the Kinesis Client Library.  If you change the App name or Stream name,
   *                 the KCL will throw errors.
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param checkpointIntervalMillis Checkpoint interval in milliseconds for Kinesis
   *                                 checkpointing (not Spark checkpointing). See the
   *                                 Kinesis Spark Streaming documentation for more
   *                                 details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @return JavaReceiverInputDStream[Array[Byte]]
   */
  def createStream(
      jssc: JavaStreamingContext, 
      appName: String, 
      streamName: String,
      endpointUrl: String,
      checkpointIntervalMillis: Long, 
      initialPositionInStream: InitialPositionInStream, 
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    jssc.receiverStream(new KinesisReceiver(appName, stream, endpoint, checkpointIntervalMillis, 
        initialPositionInStream, storageLevel))
  }
}
