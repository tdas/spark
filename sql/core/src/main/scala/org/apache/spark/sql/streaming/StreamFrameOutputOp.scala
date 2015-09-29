package org.apache.spark.sql.streaming

import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

class StreamFrameOutputOp(streamFrame: StreamFrame, outputOp: DStream[Row] => Unit) {

  streamFrame.sqlStreamingContext.outputOps += this

  def execute(): Unit = {
    val streamingContext = streamFrame.sqlStreamingContext
    outputOp(streamFrame.dstream)
  }
}
