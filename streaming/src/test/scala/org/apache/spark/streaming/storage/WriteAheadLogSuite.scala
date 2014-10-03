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
import java.nio.ByteBuffer
import java.util.Random

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration

import org.apache.spark.streaming.TestSuiteBase

class WriteAheadLogSuite extends TestSuiteBase {
  
  val hadoopConf = new Configuration()
  val random = new Random()

  test("Test successful writes") {
    val dir = Files.createTempDir()
    val file = new File(dir, "TestWriter")
    try {
      val dataToWrite = for (i <- 1 to 50) yield generateRandomData()
      val writer = new WriteAheadLogWriter("file:///" + file.toString, hadoopConf)
      val segments = dataToWrite.map(writer.write)
      writer.close()
      val writtenData = readData(segments, file)
      assert(writtenData.toArray === dataToWrite.toArray)
    } finally {
      file.delete()
      dir.delete()
    }
  }

  test("Test successful reads using random reader") {
    val file = File.createTempFile("TestRandomReads", "")
    file.deleteOnExit()
    val writtenData = writeData(50, file)
    val reader = new WriteAheadLogRandomReader("file:///" + file.toString, hadoopConf)
    writtenData.foreach {
      x =>
        val length = x._1.remaining()
        assert(x._1 === reader.read(new FileSegment(file.toString, x._2, length)))
    }
    reader.close()
  }

  test("Test reading data using random reader written with writer") {
    val dir = Files.createTempDir()
    val file = new File(dir, "TestRandomReads")
    try {
      val dataToWrite = for (i <- 1 to 50) yield generateRandomData()
      val segments = writeUsingWriter(file, dataToWrite)
      val iter = dataToWrite.iterator
      val reader = new WriteAheadLogRandomReader("file:///" + file.toString, hadoopConf)
      val writtenData = segments.map { x =>
        reader.read(x)
      }
      assert(dataToWrite.toArray === writtenData.toArray)
    } finally {
      file.delete()
      dir.delete()
    }
  }

  test("Test successful reads using sequential reader") {
    val file = File.createTempFile("TestSequentialReads", "")
    file.deleteOnExit()
    val writtenData = writeData(50, file)
    val reader = new WriteAheadLogReader("file:///" + file.toString, hadoopConf)
    val iter = writtenData.iterator
    iter.foreach { x =>
      assert(reader.hasNext === true)
      assert(reader.next() === x._1)
    }
    reader.close()
  }


  test("Test reading data using sequential reader written with writer") {
    val dir = Files.createTempDir()
    val file = new File(dir, "TestWriter")
    try {
      val dataToWrite = for (i <- 1 to 50) yield generateRandomData()
      val segments = writeUsingWriter(file, dataToWrite)
      val iter = dataToWrite.iterator
      val reader = new WriteAheadLogReader("file:///" + file.toString, hadoopConf)
      reader.foreach { x =>
        assert(x === iter.next())
      }
    } finally {
      file.delete()
      dir.delete()
    }
  }

  /**
   * Writes data to the file and returns the an array of the bytes written.
   * @param count
   * @return
   */
  // We don't want to be using the WAL writer to test the reader - it will be painful to figure
  // out where the bug is. Instead generate the file by hand and see if the WAL reader can
  // handle it.
  def writeData(count: Int, file: File): ArrayBuffer[(ByteBuffer, Long)] = {
    val writtenData = new ArrayBuffer[(ByteBuffer, Long)]()
    val writer = new RandomAccessFile(file, "rw")
    var i = 0
    while (i < count) {
      val data = generateRandomData()
      writtenData += ((data, writer.getFilePointer))
      data.rewind()
      writer.writeInt(data.remaining())
      writer.write(data.array())
      i += 1
    }
    writer.close()
    writtenData
  }

  def readData(segments: Seq[FileSegment], file: File): Seq[ByteBuffer] = {
    val reader = new RandomAccessFile(file, "r")
    segments.map { x =>
      reader.seek(x.offset)
      val data = new Array[Byte](x.length)
      reader.readInt()
      reader.readFully(data)
      ByteBuffer.wrap(data)
    }
  }

  def generateRandomData(): ByteBuffer = {
    val data = new Array[Byte](random.nextInt(50))
    random.nextBytes(data)
    ByteBuffer.wrap(data)
  }

  def writeUsingWriter(file: File, input: Seq[ByteBuffer]): Seq[FileSegment] = {
    val writer = new WriteAheadLogWriter(file.toString, hadoopConf)
    val segments = input.map(writer.write)
    writer.close()
    segments
  }
}
