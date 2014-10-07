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
package org.apache.spark.streaming.storage

import java.io.Closeable
import java.lang.reflect.Method
import java.nio.ByteBuffer

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream

private[streaming] class WriteAheadLogWriter(path: String, conf: Configuration) extends Closeable {
  private val stream = HdfsUtils.getOutputStream(path, conf)
  private var nextOffset = stream.getPos
  private var closed = false
  private val hflushMethod = getHflushOrSync()

  // Data is always written as:
  // - Length - Long
  // - Data - of length = Length
  def write(data: ByteBuffer): FileSegment = synchronized {
    assertOpen()
    data.rewind() // Rewind to ensure all data in the buffer is retrieved
    val lengthToWrite = data.remaining()
    val segment = new FileSegment(path, nextOffset, lengthToWrite)
    stream.writeInt(lengthToWrite)
    if (data.hasArray) {
      stream.write(data.array())
    } else {
      // If the buffer is not backed by an array we need to write the data byte by byte
      while (data.hasRemaining) {
        stream.write(data.get())
      }
    }
    hflushOrSync()
    nextOffset = stream.getPos
    segment
  }

  override private[streaming] def close(): Unit = synchronized {
    closed = true
    hflushOrSync()
    stream.close()
  }

  private def hflushOrSync() {
    stream.getWrappedStream.flush()
    hflushMethod.foreach(_.invoke(stream))
  }

  private def getHflushOrSync(): Option[Method] = {
    Try {
      Some(classOf[FSDataOutputStream].getMethod("hflush"))
    }.recover {
      case e: NoSuchMethodException =>
        Some(classOf[FSDataOutputStream].getMethod("sync"))
    }.getOrElse(None)
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Writer to write to file.")
  }
}
