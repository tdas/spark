package org.apache.spark.sql.streaming.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.streaming.dstream.DStream

case class Project(projectList: Seq[NamedExpression], child: StreamingPlan) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def execute(): DStream[InternalRow] = {
    val inputSchema = child.output
    val expressions = projectList
    child.execute().mapPartitions { iter =>
      val projection = new InterpretedMutableProjection(expressions, inputSchema)
      iter.map { row => projection(row) }
    }
  }
}

case class Filter(condition: Expression, child: StreamingPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def execute(): DStream[InternalRow] = {
    val attributes = child.output
    val expression = condition
    child.execute().mapPartitions { iter =>
      val predicate = InterpretedPredicate.create(expression, attributes)
      iter.filter { row => predicate(row) }
    }
  }
}

case class PhysicalDStream(output: Seq[Attribute], dstream: DStream[InternalRow]) extends LeafNode {
  override def execute(): DStream[InternalRow] = dstream
}
