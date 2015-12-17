package org.apache.spark.sql.streaming.execution

import org.apache.spark.sql.catalyst.planning.GenericStrategy
import org.apache.spark.sql.streaming.SQLStreamingContext

/**
 * Created by tdas on 9/25/15.
 */
class StreamingPlanner(val sqlStreamingContext: SQLStreamingContext)
  extends StreamingStrategies {

  /** A list of execution strategies that can be used by the planner */
  override def strategies: Seq[GenericStrategy[StreamingPlan]] = {
    BasicOperatorsStrategy :: Nil
  }
}
