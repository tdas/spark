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

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import scala.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.util.Utils

class WriteAheadLogSuite extends FunSuite with BeforeAndAfter {
  
  val hadoopConf = new Configuration()
  var tempDirectory: File = null

  before {
    tempDirectory = Files.createTempDir()
  }

  after {
    if (tempDirectory != null && tempDirectory.exists()) {
      FileUtils.deleteDirectory(tempDirectory)
      tempDirectory = null
    }
  }

  test("WriteAheadLogWriter - writing data") {
    val file = new File(tempDirectory, Random.nextString(10))
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter("file:///" + file, hadoopConf)
    val segments = dataToWrite.map(data => writer.write(data))
    writer.close()
    val writtenData = readDataFromLogManually(file, segments)
    assert(writtenData.toArray === dataToWrite.toArray)
  }

  test("WriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val file = new File(tempDirectory, Random.nextString(10))
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter("file:///" + file, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(data)
      assert(readDataFromLogManually(file, Seq(segment)).head === data)
    }
    writer.close()
  }

  test("WriteAheadLogReader - sequentially reading data") {
    val file = File.createTempFile("TestSequentialReads", "", tempDirectory)
    val writtenData = writeRandomDataToLogManually(50, file)
    val reader = new WriteAheadLogReader("file:///" + file.toString, hadoopConf)
    val readData = reader.toSeq.map(byteBufferToString)
    assert(readData.toList === writtenData.map { _._1 }.toList)
    assert(reader.hasNext === false)
    intercept[Exception] {
      reader.next()
    }
    reader.close()
  }

  test("WriteAheadLogReader - sequentially reading data written with writer") {
    val file = new File(tempDirectory, "TestWriter")
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(file, dataToWrite)
    val iter = dataToWrite.iterator
    val reader = new WriteAheadLogReader("file:///" + file.toString, hadoopConf)
    reader.foreach { byteBuffer =>
      assert(byteBufferToString(byteBuffer) === iter.next())
    }
  }

  test("WriteAheadLogRandomReader - reading data using random reader") {
    val file = File.createTempFile("TestRandomReads", "", tempDirectory)
    val writtenData = writeRandomDataToLogManually(50, file)
    val reader = new WriteAheadLogRandomReader("file:///" + file.toString, hadoopConf)
    val reorderedWrittenData = writtenData.toSeq.permutations.next()
    reorderedWrittenData.foreach { case (data, offset, length) =>
      val segment = new FileSegment(file.toString, offset, length)
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogRandomReader - reading data using random reader written with writer") {
    val file = new File(tempDirectory, "TestRandomReads")
    val dataToWrite = generateRandomData()
    val segments = writeDataUsingWriter(file, dataToWrite)
    val reader = new WriteAheadLogRandomReader("file:///" + file.toString, hadoopConf)
    val writtenData = segments.map { byteBuffer => byteBufferToString(reader.read(byteBuffer)) }
    assert(dataToWrite.toList === writtenData.toList)
  }

  test("WriteAheadLogManager - write rotating logs and reading from them") {
    val fakeClock = new ManualClock
    val manager = new WriteAheadLogManager(tempDirectory.toString, hadoopConf,
      rollingIntervalSecs = 1, clock = fakeClock)
    val dataToWrite = generateRandomData(10)
    dataToWrite.foreach { data =>
      fakeClock.addToTime(500)
      manager.writeToLog(data)
    }
    println(tempDirectory.list().mkString("\n"))
    assert(tempDirectory.list().size > 1)

    val readData = manager.readFromLog().map(byteBufferToString).toSeq
    println("Generated data")
    printData(dataToWrite)
    println("Read data")
    printData(readData)
    assert(dataToWrite.toList === readData.toList)
  }

  /**
   * Write data to the file and returns the an array of the bytes written.
   * This is used to test the WAL reader independently of the WAL writer.
   */
  def writeRandomDataToLogManually(count: Int, file: File): ArrayBuffer[(String, Long, Int)] = {
    val writtenData = new ArrayBuffer[(String, Long, Int)]()
    val writer = new RandomAccessFile(file, "rw")
    for (i <- 1 to count) {
      val data = generateRandomData(1).head
      val offset = writer.getFilePointer()
      val bytes = Utils.serialize(data)
      writer.writeInt(bytes.size)
      writer.write(bytes)
      writtenData += ((data, offset, bytes.size))
    }
    writer.close()
    writtenData
  }

  /**
   * Read data from the given segments of log file and returns the read list of byte buffers.
   * This is used to test the WAL writer independently of the WAL reader.
   */
  def readDataFromLogManually(file: File, segments: Seq[FileSegment]): Seq[String] = {
    val reader = new RandomAccessFile(file, "r")
    println("File " + file + " has " + reader.length() + " bytes ")
    segments.map { x =>
      reader.seek(x.offset)
      val data = new Array[Byte](x.length)
      reader.readInt()
      reader.readFully(data)
      Utils.deserialize[String](data)
    }
  }

  def generateRandomData(numItems: Int = 50, itemSize: Int = 50): Seq[String] = {
    (1 to numItems).map { _.toString }
  }

  def printData(data: Seq[String]) {
    println("# items in data = " + data.size)
    println(data.mkString("\n"))
  }

  def writeDataUsingWriter(file: File, input: Seq[String]): Seq[FileSegment] = {
    val writer = new WriteAheadLogWriter(file.toString, hadoopConf)
    val segments = input.map { data => writer.write(data) }
    writer.close()
    segments
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }
}
