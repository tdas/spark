package org.apache.spark.sql.streaming.execution

import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.logical.LogicalDStream

abstract class StreamingStrategies extends QueryPlanner[StreamingPlan] {

  object BasicOperators extends Strategy {
    override def apply(plan: LogicalPlan): Seq[StreamingPlan] = plan match {
      case logical.Project(projectList, child) =>
        Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        Filter(condition, planLater(child)) :: Nil
      case LogicalDStream(attributes, dstream) =>
        PhysicalDStream(attributes, dstream) :: Nil
    }
  }
}
