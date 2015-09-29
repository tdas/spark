package org.apache.spark.sql.streaming.logical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.streaming.dstream.DStream

case class LogicalDStream(output: Seq[Attribute], dstream: DStream[InternalRow])
  extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): LogicalDStream.this.type =
    LogicalDStream(output.map(_.newInstance()), dstream).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = false
}
