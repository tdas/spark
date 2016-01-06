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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.ReceiverSource
import org.apache.spark.streaming.receiver.Receiver

private[kinesis] case class KinesisSource(
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream,
    awsCredentialsOption: Option[SerializableAWSCredentials] = None)
  extends ReceiverSource[Array[Byte]]() {

  @transient val credentials = awsCredentialsOption.getOrElse {
    new DefaultAWSCredentialsProviderChain().getCredentials()
  }

  @transient private val client = new AmazonKinesisClient(credentials)
  client.setEndpoint(endpointUrl, "kinesis", regionName)

  override def receivers: Seq[Receiver[Array[Byte]] = {
    val numReceivers = client.describeStream(streamName).getStreamDescription.getShards.size()
    Seq.fill(numReceivers) {
      new KinesisReceiver(streamName, endpointUrl, regionName, initialPositionInStream, )
    }
  }

  override def getSlice(sqlContext: SQLContext, start: Option[Offset], end: Offset): RDD[InternalRow] = {
    val blockInfos = getBlocksByRange(start, end)
    // This returns true even for when blockInfos is empty
    val allBlocksHaveRanges = blockInfos.map { _.metadataOption }.forall(_.nonEmpty)

    if (allBlocksHaveRanges) {
      // Create a KinesisBackedBlockRDD, even when there are no blocks
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
      val seqNumRanges = blockInfos.map {
        _.metadataOption.get.asInstanceOf[SequenceNumberRanges] }.toArray
      val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
      val blockRDD = new KinesisBackedBlockRDD(
        sqlContext.sparkContext, regionName, endpointUrl, blockIds, seqNumRanges,
        isBlockIdValid = isBlockIdValid,
        awsCredentialsOption = awsCredentialsOption)
      val encodingFunc = encoder.toRow _
      blockRDD.map { encodingFunc }
    } else {
      super.getSlice(sqlContext, start, end)
    }
  }
}
