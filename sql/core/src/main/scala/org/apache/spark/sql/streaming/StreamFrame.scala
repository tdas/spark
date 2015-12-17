package org.apache.spark.sql.streaming

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, Explode, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.{logical => catalystLogical}
import org.apache.spark.sql.streaming.execution.StreamingPlan
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.streaming.dstream.DStream


class StreamFrame(
    @transient val sqlStreamingContext: SQLStreamingContext,
    @transient logicalPlan: LogicalPlan) {

  private lazy val physicalPlan: StreamingPlan = {
    val analyzedPlan = sqlStreamingContext.sqlContext.analyzer.execute(logicalPlan)
    val optimizedPlan = sqlStreamingContext.sqlContext.optimizer.execute(analyzedPlan)
    sqlStreamingContext.planner.plan(optimizedPlan).next()
  }

  def select(cols: Column*): StreamFrame = {
    val namedExpressions = cols.map {
      // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
      // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
      // make it a NamedExpression.
      case Column(u: UnresolvedAttribute) => UnresolvedAlias(u)
      case Column(expr: NamedExpression) => expr
      // Leave an unaliased explode with an empty list of names since the analyzer will generate the
      // correct defaults after the nested expression's type has been resolved.
      case Column(explode: Explode) => MultiAlias(explode, Nil)
      case Column(expr: Expression) => Alias(expr, expr.prettyString)()
    }
    catalystLogical.Project(namedExpressions.toSeq, logicalPlan)
  }

  def selectExpr(exprs: String*): StreamFrame = {
    select(exprs.map { expr =>
      Column(SqlParser.parseExpression(expr))
    }: _*)
  }


  def filter(condition: Column): StreamFrame = catalystLogical.Filter(condition.expr, logicalPlan)

  def filter(conditionExpr: String): StreamFrame = {
    filter(Column(SqlParser.parseExpression(conditionExpr)))
  }

  def print(): Unit = {
    new StreamFrameOutputOp(this, dstream => dstream.print())
  }

  def dstream(): DStream[Row] = {
    physicalPlan.execute().asInstanceOf[DStream[Row]]
  }

  @inline private implicit def logicalPlanToStreamFrame(logicalPlan: LogicalPlan): StreamFrame = {
    new StreamFrame(sqlStreamingContext, logicalPlan)
  }
}
