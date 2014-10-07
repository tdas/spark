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
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkEnv, TaskContext, SparkContext}
import org.apache.spark.storage.{BlockId, StreamBlockId, StorageLevel}
import org.apache.spark.streaming.TestSuiteBase

class HDFSBackedBlockRDDSuite extends TestSuiteBase {
  val sparkContext = new SparkContext(conf)
  val hadoopConf = new Configuration()
  val blockIdCounter = new AtomicInteger(0)
  val streamCounter = new AtomicInteger(0)
  val blockManager = sparkContext.env.blockManager
  var file: File = null
  var dir: File = null

  override def beforeFunction() {
    super.beforeFunction()
    dir = Files.createTempDir()
    file = new File(dir, "BlockManagerWrite")
  }

  override def afterFunction() {
    super.afterFunction()
    file.delete()
    dir.delete()
  }

  test("Verify all data is available when part of the data is only on HDFS") {
    doTestHDFSWrites(writeAllToBM = false, 20, 5)
  }

  test("Verify all data is available when all data is in BM") {
    doTestHDFSWrites(writeAllToBM = true, 20, 5)
  }

  test("Verify all data is available when part of the data is in BM with one string per block") {
    doTestHDFSWrites(writeAllToBM = false, 20, 20)
  }

  test("Verify all data is available when all data is in BM with one string per block") {
    doTestHDFSWrites(writeAllToBM = true, 20, 20)
  }

  /**
   * Write a bunch of events into the HDFS Block RDD. Put a part of all of them to the
   * BlockManager, so all reads need not happen from HDFS.
   * @param writeAllToBM - If true, all data is written to BlockManager
   * @param total - Total number of Strings to write
   * @param blockCount - Number of blocks to write (therefore, total # of events per block =
   *                   total/blockCount
   */
  private def doTestHDFSWrites(writeAllToBM: Boolean, total: Int, blockCount: Int) {
    val countPerBlock = total / blockCount
    val blockIds = (0 until blockCount).map { _ =>
      StreamBlockId(streamCounter.incrementAndGet(), blockIdCounter.incrementAndGet())
    }

    val (writtenStrings, segments) = writeDataToHDFS(total, countPerBlock, file, blockIds)

    for (i <- 0 until writtenStrings.length) {
      if (i % 2 == 0 || writeAllToBM) {
        blockManager.putIterator(blockIds(i), writtenStrings(i).iterator,
          StorageLevel.MEMORY_ONLY)
      }
    }

    val rdd = new HDFSBackedBlockRDD[String](sparkContext, hadoopConf, blockIds.toArray,
      segments.toArray, StorageLevel.MEMORY_ONLY)
    val partitions = rdd.getPartitions
    // The task context is not used in this RDD directly, so ok to be null
    val dataFromRDD = partitions.flatMap(rdd.compute(_, null))
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

    val strings: Seq[String] = (0 until count).map(_ => RandomStringUtils.randomAlphabetic(50))

    var writerOpt: Option[WriteAheadLogWriter] = None
    try {
      writerOpt = Some(new WriteAheadLogWriter(file.toString, hadoopConf))
      val writer = writerOpt.get
      val blockData =
        0.until(count, countPerBlock).map(y => (0 until countPerBlock).map(x => strings(x + y)))
      val blockIdIter = blockIds.iterator
      (blockData, blockData.map {
        x =>
          writer.write(blockManager.dataSerialize(blockIdIter.next(), x.iterator))
      })
    } finally {
      writerOpt.foreach(_.close())
    }
  }
}
