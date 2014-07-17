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
package org.apache.spark.streaming.flume


import java.io.{ObjectOutput, ObjectInput, Externalizable}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, Executors}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.flume.sink._


/**
 * A [[ReceiverInputDStream]] that can be used to read data from several Flume agents running
 * [[org.apache.spark.flume.sink.SparkSink]]s.
 * @param _ssc Streaming context that will execute this input stream
 * @param addresses List of addresses at which SparkSinks are listening
 * @param maxBatchSize Maximum size of a batch
 * @param parallelism Number of parallel connections to open
 * @param storageLevel The storage level to use.
 * @tparam T Class type of the object of this stream
 */
private[streaming]
class FlumePollingInputDStream[T: ClassTag](
    @transient _ssc: StreamingContext,
    val addresses: Seq[InetSocketAddress],
    val maxBatchSize: Int,
    val parallelism: Int,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[SparkFlumePollingEvent](_ssc) {

  override def getReceiver(): Receiver[SparkFlumePollingEvent] = {
    new FlumePollingReceiver(addresses, maxBatchSize, parallelism, storageLevel)
  }
}

private[streaming]
class FlumePollingReceiver(
    addresses: Seq[InetSocketAddress],
    maxBatchSize: Int,
    parallelism: Int,
    storageLevel: StorageLevel
  ) extends Receiver[SparkFlumePollingEvent](storageLevel) with Logging {

  lazy val channelFactoryExecutor =
    Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).
      setNameFormat("Flume Receiver Channel Thread - %d").build())

  lazy val channelFactory =
    new NioClientSocketChannelFactory(channelFactoryExecutor, channelFactoryExecutor)

  lazy val receiverExecutor = Executors.newFixedThreadPool(parallelism,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Flume Receiver Thread - %d").build())

  private val connections = new LinkedBlockingQueue[FlumeConnection]()

  override def onStart(): Unit = {
    // Create the connections to each Flume agent.
    addresses.foreach(host => {
      val transceiver = new NettyTransceiver(host, channelFactory)
      val client = SpecificRequestor.getClient(classOf[SparkFlumeProtocol.Callback], transceiver)
      connections.add(new FlumeConnection(transceiver, client))
    })
    for (i <- 0 until parallelism) {
      logInfo("Starting Flume Polling Receiver worker threads starting..")
      // Threads that pull data from Flume.
      receiverExecutor.submit(new Runnable {
        override def run(): Unit = {
          while (true) {
            val connection = connections.poll()
            val client = connection.client
            try {
              val eventBatch = client.getEventBatch(maxBatchSize)
              if (!SparkSinkUtils.isErrorBatch(eventBatch)) {
                // No error, proceed with processing data
                val seq = eventBatch.getSequenceNumber
                val events: java.util.List[SparkSinkEvent] = eventBatch.getEvents
                logDebug(
                  "Received batch of " + events.size() + " events with sequence number: " + seq)
                try {
                  // Convert each Flume event to a serializable SparkPollingEvent
                  var j = 0
                  while (j < events.size()) {
                    store(SparkFlumePollingEvent.fromSparkSinkEvent(events(j)))
                    logDebug("Stored events with seq:" + seq)
                    j += 1
                  }
                  logDebug("Sending ack for sequence number: " + seq)
                  // Send an ack to Flume so that Flume discards the events from its channels.
                  client.ack(seq)
                  logDebug("Ack sent for sequence number: " + seq)
                } catch {
                  case e: Exception =>
                    try {
                      // Let Flume know that the events need to be pushed back into the channel.
                      logDebug("Sending nack for sequence number: " + seq)
                      client.nack(seq) // If the agent is down, even this could fail and throw
                      logDebug("Nack sent for sequence number: " + seq)
                    } catch {
                      case e: Exception => logError(
                        "Sending Nack also failed. A Flume agent is down.")
                    }
                    TimeUnit.SECONDS.sleep(2L) // for now just leave this as a fixed 2 seconds.
                    logWarning("Error while attempting to store events", e)
                }
              } else {
                logWarning("Did not receive events from Flume agent due to error on the Flume " +
                  "agent: " + eventBatch.getErrorMsg)
              }
            } catch {
              case e: Exception =>
                logWarning("Error while reading data from Flume", e)
            } finally {
              connections.add(connection)
            }
          }
        }
      })
    }
  }

  override def onStop(): Unit = {
    logInfo("Shutting down Flume Polling Receiver")
    receiverExecutor.shutdownNow()
    connections.foreach(connection => {
      connection.transceiver.close()
    })
    channelFactory.releaseExternalResources()
  }
}

/**
 * A wrapper around the transceiver and the Avro IPC API. 
 * @param transceiver The transceiver to use for communication with Flume
 * @param client The client that the callbacks are received on.
 */
private class FlumeConnection(val transceiver: NettyTransceiver,
  val client: SparkFlumeProtocol.Callback)

/**
 * Companion object of [[SparkFlumePollingEvent]]
 */
private[streaming] object SparkFlumePollingEvent {
  def fromSparkSinkEvent(in: SparkSinkEvent): SparkFlumePollingEvent = {
    val event = new SparkFlumePollingEvent()
    event.event = in
    event
  }
}

/*
 * Unfortunately Avro does not allow including pre-compiled classes - so even though
 * SparkSinkEvent is identical to AvroFlumeEvent, we need to create a new class and a wrapper
 * around that to make it externalizable.
 */
class SparkFlumePollingEvent extends Externalizable with Logging {
  var event: SparkSinkEvent = new SparkSinkEvent()

  /* De-serialize from bytes. */
  def readExternal(in: ObjectInput) {
    val (headers, bodyBuff) = EventTransformer.readExternal(in)
    event.setBody(ByteBuffer.wrap(bodyBuff))
    event.setHeaders(headers)
  }

  /* Serialize to bytes. */
  def writeExternal(out: ObjectOutput) {
    EventTransformer.writeExternal(out, event.getHeaders, event.getBody.array())
  }
}


