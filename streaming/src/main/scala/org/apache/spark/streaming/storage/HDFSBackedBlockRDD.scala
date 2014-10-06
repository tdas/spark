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

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{StorageLevel, BlockId}
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}

private[spark]
class HDFSBackedBlockRDDPartition(val blockId: BlockId, idx: Int, val segment: FileSegment)
  extends Partition {
  val index = idx
}

private[spark]
class HDFSBackedBlockRDD[T: ClassTag](
    @transient sc: SparkContext,
    hadoopConf: Configuration,
    @transient override val blockIds: Array[BlockId],
    @transient val segments: Array[FileSegment],
    val storageLevel: StorageLevel
  ) extends BlockRDD[T](sc, blockIds) {

  override def getPartitions: Array[Partition] = {
    assertValid()
    (0 until blockIds.size).map { i =>
      new HDFSBackedBlockRDDPartition(blockIds(i), i, segments(i))
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[HDFSBackedBlockRDDPartition]
    val blockId = partition.blockId
    blockManager.get(blockId) match {
      // Data is in Block Manager, grab it from there.
      case Some(block) => block.data.asInstanceOf[Iterator[T]]
      // Data not found in Block Manager, grab it from HDFS
      case None =>
        // TODO: Perhaps we should cache readers at some point?
        val reader = new WriteAheadLogRandomReader(partition.segment.path, hadoopConf)
        val dataRead = reader.read(partition.segment)
        reader.close()
        val data = blockManager.dataDeserialize(blockId, dataRead).asInstanceOf[Iterator[T]]
        blockManager.putIterator(blockId, data, storageLevel)
        data
    }
  }
}
