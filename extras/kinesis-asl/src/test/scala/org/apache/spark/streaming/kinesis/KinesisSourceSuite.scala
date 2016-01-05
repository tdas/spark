package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.test.SharedSQLContext


class KinesisSourceSuite extends QueryTest with SharedSQLContext with KinesisFunSuite {

  import testImplicits._

  private var testUtils: KPLBasedKinesisTestUtils = _
  private var streamName: String = _

  override val streamingTimout = 30.seconds

  case class AddKinesisData(kinesisSource: KinesisSource, data: Int*) extends AddData {
    override def addData(): Offset = {
      val shardIdToSeqNums = testUtils.pushData(data, false).map { case (shardId, info) =>
        (StreamAndShard(streamName, shardId), info.last._2)
      }
      assert(shardIdToSeqNums.size === testUtils.streamShardCount,
        s"Data must be send to all ${testUtils.streamShardCount} shards of stream $streamName")
      KinesisSourceOffset(shardIdToSeqNums)
    }

    override def source: Source = kinesisSource
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KPLBasedKinesisTestUtils
    testUtils.createStream()
    streamName = testUtils.streamName
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.deleteStream()
    }
    super.afterAll()
  }

  test("basic receiving") {
    val kinesisSource = KinesisSource(
      testUtils.regionName, testUtils.endpointUrl, Set(streamName), InitialPositionInStream.LATEST, None)
    val mapped = kinesisSource.toDS().map[Int]((bytes: Array[Byte]) => new String(bytes).toInt + 1)
    val testData = 1 to 10  // This ensures that data is sent to multiple shards for 2 shard streams
    testStream(mapped)(
      AddKinesisData(kinesisSource, testData:_*),
      CheckAnswer(testData.map { _ + 1}:_*)
    )
  }
}
