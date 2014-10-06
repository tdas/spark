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
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.streaming.TestSuiteBase

class HDFSBackedBlockRDDSuite extends TestSuiteBase {
  val sparkContext = new SparkContext(conf)
  val hadoopConf = new Configuration()

  test("block manager has data") {
    val dir = Files.createTempDir()
    val file = new File(dir, "BlockManagerWrite")

  }

  private def writeDataToHDFS(count: Int, file: File): Seq[String] = {
    val str: Seq[String] = for (i <- 1 to count) yield RandomStringUtils.random(50)

    var writerOpt: Option[WriteAheadLogWriter] = None
    try {
      writerOpt = Some(new WriteAheadLogWriter(file.toString, hadoopConf))
      val writer = writerOpt.get

    } finally {
      writerOpt.foreach(_.close())
    }
    str
  }



}
