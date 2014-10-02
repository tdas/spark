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

import java.io.File

import com.google.common.io.Files

import org.apache.spark.streaming.TestSuiteBase

class WriteAheadLogReaderSuite extends TestSuiteBase {

  test("Test success") {
    val file = File.createTempFile("testSuccessFulReads", "")
    file.deleteOnExit()
    val writtenData = TestUtils.writeData(50, file)
    val reader = new WriteAheadLogReader("file:///" + file.toString)
    val iter = writtenData.iterator
    iter.foreach { x =>
      assert(reader.hasNext === true)
      assert(reader.next() === x._1)
    }
    reader.close()
  }


  test("Test reading data written with writer") {
    val dir = Files.createTempDir()
    val file = new File(dir, "TestWriter")
    try {
      val dataToWrite = for (i <- 1 to 50) yield TestUtils.generateRandomData()
      val segments = TestUtils.writeUsingWriter(file, dataToWrite)
      val iter = dataToWrite.iterator
      val reader = new WriteAheadLogReader("file:///" + file.toString)
      reader.foreach { x =>
        assert(x === iter.next())
      }
    } finally {
      file.delete()
      dir.delete()
    }
  }
}
