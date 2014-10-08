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
package org.apache.spark.streaming.storage.rdd

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.storage.{BlockId, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.storage.{FileSegment, WriteAheadLogWriter}
import org.apache.spark.{SparkConf, SparkContext}

class HDFSBackedBlockRDDSuite extends FunSuite with BeforeAndAfter {
  // Name of the framework for Spark context
  def framework = this.getClass.getSimpleName

  // Master for Spark context
  def master = "local[2]"

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)
  val sparkContext = new SparkContext(conf)
  val hadoopConf = new Configuration()
  val blockManager = sparkContext.env.blockManager
  var file: File = null
  var dir: File = null

  before {
    dir = Files.createTempDir()
    file = new File(dir, "BlockManagerWrite")
  }

  after {
    file.delete()
    dir.delete()
  }

  test("Verify all data is available when all data is in BM") {
    doTestHDFSBackedRDD(writeAllToBM = true, 20, 5)
  }


  test("Verify all data is available when all data is in BM with one string per block") {
    doTestHDFSBackedRDD(writeAllToBM = true, 20, 20)
  }

  test("Verify all data is available no data is in BM") {
    doTestHDFSBackedRDD(writeAllToBM = false, 20, 5)
  }

  test("Verify all data is available no data is in BM with one string per block") {
    doTestHDFSBackedRDD(writeAllToBM = false, 20, 20)
  }


  /**
   * Write a bunch of events into the HDFS Block RDD. Put a part of all of them to the
   * BlockManager, so all reads need not happen from HDFS.
   * @param writeAllToBM - If true, all data is written to BlockManager
   * @param total - Total number of Strings to write
   * @param blockCount - Number of blocks to write (therefore, total # of events per block =
   *                   total/blockCount
   */
  private def doTestHDFSBackedRDD(writeAllToBM: Boolean, total: Int, blockCount: Int) {
    val countPerBlock = total / blockCount
    val blockIds = (0 until blockCount).map(i => StreamBlockId(i, i))

    val (writtenStrings, segments) = writeDataToHDFS(total, countPerBlock, file, blockIds)

    val rdd = new HDFSBackedBlockRDD[String](sparkContext, hadoopConf, blockIds.toArray,
      segments.toArray, false, StorageLevel.MEMORY_ONLY)

    // The task context is not used in this RDD directly, so ok to be null
    val dataFromRDD = rdd.collect()
    // verify each partition is equal to the data pulled out
    assert(writtenStrings.flatten === dataFromRDD)
  }

  /**
   * Write data to HDFS and get a list of Seq of Seqs in which each Seq represents the data that
   * went into one block.
   * @param count - Number of Strings to write
   * @param countPerBlock - Number of Strings per block
   * @param file - The file to write to
   * @param blockIds - List of block ids to use.
   * @return - Tuple of (Seq of Seqs, each of these Seqs is one block, Seq of FileSegments,
   *         each representing the block being written to HDFS.
   */
  private def writeDataToHDFS(
    count: Int,
    countPerBlock: Int,
    file: File,
    blockIds: Seq[BlockId]
    ): (Seq[Seq[String]], Seq[FileSegment]) = {

    assert(count / countPerBlock === blockIds.size)

    val strings = (0 until count).map { _ => scala.util.Random.nextString(50)}

    val writer = new WriteAheadLogWriter(file.toString, hadoopConf)
    val blockData = strings.grouped(countPerBlock).toSeq
    val segments = new ArrayBuffer[FileSegment]()
    blockData.zip(blockIds).foreach {
      case (data, id) =>
        segments += writer.write(blockManager.dataSerialize(id, data.iterator))
    }
    writer.close()
    (blockData, segments.toSeq)
  }
}
