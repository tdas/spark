package org.apache.spark.streaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.storage.{StorageLevel, BlockManager, BlockId, StreamBlockId}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.{SparkException, SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KinesisBackedBlockRDDSuite extends FunSuite with BeforeAndAfterAll {

  private val streamName = "TDTest"
  private val regionId = "us-east-1"
  private val endPointUrl = "https://kinesis.us-east-1.amazonaws.com"

  private val proxy = new KinesisProxy(
    new DefaultAWSCredentialsProviderChain(), endPointUrl, regionId)
  private val shardInfo = proxy.getAllShards(streamName)
  require(shardInfo.size > 1, "Need a stream with more than 1 shard")

  private val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
  kinesisClient.setEndpoint(endPointUrl)

  private val testData = 1 to 8

  private var sc: SparkContext = null
  private var blockManager: BlockManager = null
  private var shardIdToDataAndSeqNumbers: Map[String, Seq[(Int, String)]] = null
  private var shardIds: Seq[String] = null
  private var shardIdToData: Map[String, Seq[Int]] = null
  private var shardIdToSeqNumbers: Map[String, Seq[String]] = null
  private var shardIdToRange: Map[String, SequenceNumberRange] = null
  private var allRanges: Seq[SequenceNumberRange] = null

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("KinesisBackedBlockRDDSuite")
    sc = new SparkContext(conf)
    blockManager = sc.env.blockManager

    shardIdToDataAndSeqNumbers = pushData(testData)
    assert(shardIdToDataAndSeqNumbers.size > 1, "Need at least 2 shards to test")

    shardIds = shardIdToDataAndSeqNumbers.keySet.toSeq
    shardIdToData = shardIdToDataAndSeqNumbers.mapValues { _.map { _._1 }}
    shardIdToSeqNumbers = shardIdToDataAndSeqNumbers.mapValues { _.map { _._2 }}
    shardIdToRange = shardIdToSeqNumbers.map { case (shardId, seqNumbers) =>
      (shardId, SequenceNumberRange(streamName, shardId, seqNumbers.head, seqNumbers.last))
    }.toMap
    allRanges = shardIdToRange.values.toSeq
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  if (KinesisTestUtils.enableTests) {
    test("Basic reading from Kinesis") {
      // Verify all data using multiple ranges in a single RDD partition
      val receivedData1 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl,
        fakeBlockIds(1),
        Array(SequenceNumberRanges(allRanges.toArray))
      ).map { bytes => new String(bytes).toInt }.collect()
      assert(receivedData1.toSet === testData.toSet)

      // Verify all data using one range in each of the multiple RDD partitions
      val receivedData2 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl,
        fakeBlockIds(allRanges.size),
        allRanges.map { range => SequenceNumberRanges(Array(range)) }.toArray
      ).map { bytes => new String(bytes).toInt }.collect()
      assert(receivedData2.toSet === testData.toSet)

      // Verify ordering within each partition
      val receivedData3 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl,
        fakeBlockIds(allRanges.size),
        allRanges.map { range => SequenceNumberRanges(Array(range)) }.toArray
      ).map { bytes => new String(bytes).toInt }.collectPartitions()
      assert(receivedData3.length === allRanges.size)
      for (i <- 0 until allRanges.size) {
        assert(receivedData3(i).toSeq === shardIdToData(allRanges(i).shardId))
      }
    }

    test("Read data available in both block manager and Kinesis") {
      testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 2)
    }

    test("Read data available only in block manager, not in Kinesis") {
      testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 0)
    }

    test("Read data available only in Kinesis, not in block manager") {
      testRDD(numPartitions = 2, numPartitionsInBM = 0, numPartitionsInKinesis = 2)
    }

    test("Read data available partially in block manager, rest in Kinesis") {
      testRDD(numPartitions = 2, numPartitionsInBM = 1, numPartitionsInKinesis = 1)
    }

    test("Test isBlockValid skips block fetching from block manager") {
      testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 0,
        testIsBlockValid = true)
    }

    test("Test whether RDD is valid after removing blocks from block anager") {
      testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 2,
        testBlockRemove = true)
    }
  }

  /**
   * Test the WriteAheadLogBackedRDD, by writing some partitions of the data to block manager
   * and the rest to a write ahead log, and then reading reading it all back using the RDD.
   * It can also test if the partitions that were read from the log were again stored in
   * block manager.
   *
   *
   *
   * @param numPartitions Number of partitions in RDD
   * @param numPartitionsInBM Number of partitions to write to the BlockManager.
   *                          Partitions 0 to (numPartitionsInBM-1) will be written to BlockManager
   * @param numPartitionsInKinesis Number of partitions to write to the Kinesis.
   *                           Partitions (numPartitions - 1 - numPartitionsInKinesis) to
   *                           (numPartitions - 1) will be written to Kinesis
   * @param testIsBlockValid Test whether setting isBlockValid to false skips block fetching
   * @param testBlockRemove Test whether calling rdd.removeBlock() makes the RDD still usable with
   *                        reads falling back to the WAL
   * Example with numPartitions = 5, numPartitionsInBM = 3, and numPartitionsInWAL = 4
   *
   *   numPartitionsInBM = 3
   *   |------------------|
   *   |                  |
   *    0       1       2       3       4
   *           |                         |
   *           |-------------------------|
   *              numPartitionsInKinesis = 4
   */
  private def testRDD(
      numPartitions: Int,
      numPartitionsInBM: Int,
      numPartitionsInKinesis: Int,
      testIsBlockValid: Boolean = false,
      testBlockRemove: Boolean = false
    ): Unit = {
    require(shardIds.size > 1, "Need at least 2 shards to test")
    require(numPartitionsInBM <= shardIds.size ,
      "Number of partitions in BlockManager cannot be more than the Kinesis test shards available")
    require(numPartitionsInKinesis <= shardIds.size ,
      "Number of partitions in Kinesis cannot be more than the Kinesis test shards available")
    require(numPartitionsInBM <= numPartitions,
      "Number of partitions in BlockManager cannot be more than that in RDD")
    require(numPartitionsInKinesis <= numPartitions,
      "Number of partitions in Kinesis cannot be more than that in RDD")

    // Put necessary blocks in the block manager
    val blockIds = fakeBlockIds(numPartitions)
    blockIds.foreach(blockManager.removeBlock(_))
    (0 until numPartitionsInBM).foreach { i =>
      val blockData = shardIdToData(shardIds(i)).iterator.map { _.toString.getBytes() }
      blockManager.putIterator(blockIds(i), blockData, StorageLevel.MEMORY_ONLY)
    }

    // Create the necessary ranges to use in the RDD
    val fakeRanges = Array.fill(numPartitions - numPartitionsInKinesis)(
      SequenceNumberRanges(SequenceNumberRange("fakeStream", "fakeShardId", "xxx", "yyy")))
    val realRanges = Array.tabulate(numPartitionsInKinesis) { i =>
      val range = shardIdToRange(shardIds(i + (numPartitions - numPartitionsInKinesis)))
      SequenceNumberRanges(Array(range))
    }
    val ranges = (fakeRanges ++ realRanges)


    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    require(
      blockIds.take(numPartitionsInBM).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )

    require(
      blockIds.drop(numPartitionsInBM).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right sequence `numPartitionsInKinesis` are configured, and others are not
    require(
      ranges.takeRight(numPartitionsInKinesis).forall(_.ranges.forall(_.streamName == streamName)),
      "Incorrect configuration of RDD, expected ranges not set: "
    )

    require(
      ranges.dropRight(numPartitionsInKinesis).forall(_.ranges.forall(_.streamName != streamName)),
      "Incorrect configuration of RDD, unexpected ranges set"
    )

    val rdd = new KinesisBackedBlockRDD(sc, regionId, endPointUrl, blockIds, ranges)
    val collectedData = rdd.map { bytes =>
      new String(bytes).toInt
    }.collect()
    assert(collectedData.toSet === testData.toSet)

    // Verify that the block fetching is skipped when isBlockValid is set to false.
    // This is done by using a RDD whose data is only in memory but is set to skip block fetching
    // Using that RDD will throw exception, as it skips block fetching even if the blocks are in
    // in BlockManager.
    if (testIsBlockValid) {
      require(numPartitionsInBM === numPartitions, "All partitions must be in BlockManager")
      require(numPartitionsInKinesis === 0, "No partitions must be in Kinesis")
      val rdd2 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl, blockIds.toArray,
        ranges, isBlockIdValid = Array.fill(blockIds.length)(false))
      intercept[SparkException] {
        rdd2.collect()
      }
    }

    // Verify that the RDD is not invalid after the blocks are removed and can still read data
    // from write ahead log
    if (testBlockRemove) {
      require(numPartitions === numPartitionsInKinesis, "All partitions must be in WAL for this test")
      require(numPartitionsInBM > 0, "Some partitions must be in BlockManager for this test")
      rdd.removeBlocks()
      assert(rdd.map { bytes => new String(bytes).toInt }.collect().toSet === testData.toSet)
    }
  }

  /**
   * Push data to Kinesis stream and return a map of
   *  shardId -> seq of (data, seq number) pushed to corresponding shard
   */
  private def pushData(testData: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()

    testData.foreach { num =>
      val str = num.toString
      val putRecordRequest = new PutRecordRequest().withStreamName(streamName)
        .withData(ByteBuffer.wrap(str.getBytes()))
        .withPartitionKey(str)

      val putRecordResult = kinesisClient.putRecord(putRecordRequest)
      val shardId = putRecordResult.getShardId
      val seqNumber = putRecordResult.getSequenceNumber()
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(Int, String)]())
      sentSeqNumbers += ((num, seqNumber))
    }
    shardIdToSeqNumbers.toMap
  }

  /** Generate fake block ids */
  private def fakeBlockIds(num: Int): Array[BlockId] = {
    Array.tabulate(num) { i => new StreamBlockId(0, i) }
  }
}
