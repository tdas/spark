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

import java.io.{RandomAccessFile, File}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object TestUtils {

  val random = new Random()

  /**
   * Writes data to the file and returns the an array of the bytes written.
   * @param count
   * @return
   */
  // We don't want to be using the WAL writer to test the reader - it will be painful to figure
  // out where the bug is. Instead generate the file by hand and see if the WAL reader can
  // handle it.
  def writeData(count: Int, file: File): ArrayBuffer[(Array[Byte], Long)] = {
    val writtenData = new ArrayBuffer[(Array[Byte], Long)]()
    val writer = new RandomAccessFile(file, "rw")
    var i = 0
    while (i < count) {
      val data = generateRandomData()
      writtenData += ((data, writer.getFilePointer))
      writer.writeInt(data.length)
      writer.write(data)
      i += 1
    }
    writer.close()
    writtenData
  }

  def readData(segments: Seq[FileSegment], file: File): Seq[Array[Byte]] = {
    val reader = new RandomAccessFile(file, "r")
    segments.map { x =>
      reader.seek(x.offset)
      val data = new Array[Byte](x.length)
      reader.readInt()
      reader.readFully(data)
      data
    }
  }

  def generateRandomData(): Array[Byte] = {
    val data = new Array[Byte](random.nextInt(50))
    random.nextBytes(data)
    data
  }

  def writeUsingWriter(file: File, input: Seq[Array[Byte]]): Seq[FileSegment] = {
    val writer = new WriteAheadLogWriter(file.toString)
    val segments = input.map(writer.write)
    writer.close()
    segments
  }
}
