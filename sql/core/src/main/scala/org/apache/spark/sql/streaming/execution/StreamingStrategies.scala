package org.apache.spark.sql.streaming.execution

import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.streaming.logical.{Window, LogicalDStream}

abstract class StreamingStrategies extends QueryPlanner[StreamingPlan] {

  object BasicOperatorsStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[StreamingPlan] = plan match {
      case logical.Project(projectList, child) =>
        Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        Filter(condition, planLater(child)) :: Nil
      case LogicalDStream(attributes, dstream) =>
        PhysicalDStream(attributes, dstream) :: Nil
    }
  }

  object WindowStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[StreamingPlan] = plan match {
      case Aggregate(groupingExprs, aggregateExprs, w @ Window(windowSpec)) =>

    }



  }
}
