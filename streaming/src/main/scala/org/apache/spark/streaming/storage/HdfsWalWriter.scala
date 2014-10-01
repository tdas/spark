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

private[streaming] class HdfsWalWriter(val path: String) {
  val stream = HdfsUtils.getOutputStream(path)
  var nextOffset = stream.getPos
  var closed = false

  // Data is always written as:
  // - Length - Long
  // - Data - of length = Length
  def write(data: Array[Byte]): FileSegment = {
    assertOpen()
    synchronized {
      val segment = new FileSegment(path, nextOffset, data.length)
      stream.writeInt(data.length)
      stream.write(data)
      stream.hflush()
      nextOffset = stream.getPos
      segment
    }
  }

  def close(): Unit = {
    closed = true
    stream.close()
  }

  def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Writer to write to file.")
  }
}
