package org.apache.spark.sql.streaming.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.streaming.dstream.DStream


abstract class StreamingPlan extends QueryPlan[StreamingPlan] {
  def execute(): DStream[InternalRow] = {
    null
  }

  def outputPartitioning: Partitioning = UnknownPartitioning(0)
}

trait LeafNode extends StreamingPlan {
  override def children: Seq[StreamingPlan] = Nil

}

trait UnaryNode extends StreamingPlan {
  def child: StreamingPlan

  override def children: Seq[StreamingPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

