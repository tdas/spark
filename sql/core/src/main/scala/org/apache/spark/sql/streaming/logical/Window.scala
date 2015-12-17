package org.apache.spark.sql.streaming.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.expressions.WindowSpec

case class Window(windowSpec: WindowSpec, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
