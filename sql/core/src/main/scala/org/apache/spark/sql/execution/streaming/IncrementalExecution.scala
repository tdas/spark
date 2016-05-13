/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.OutputMode
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
class IncrementalExecution(
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    outputMode: OutputMode,
    checkpointLocation: String,
    currentBatchId: Long)
  extends QueryExecution(sparkSession, logicalPlan) {

  // Modified planner with stateful operations.
  override val planner =
    new SparkPlanner(sparkSession.sparkContext,
      sparkSession.sessionState.conf,
      Nil) {

      override def strategies: Seq[Strategy] = {
        Seq(CounterStrategy) ++ super.strategies
      }

      /**
       * Used to plan aggregation queries that are computed incrementally as part of a
       * [[org.apache.spark.sql.ContinuousQuery]]. Currently this rule is injected into the planner
       * on-demand, only when planning in a [[StreamExecution]]
       */
      object StatefulAggregationStrategy extends Strategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
          case PhysicalAggregation(
          namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

            aggregate.Utils.planStreamingAggregation(
              namedGroupingExpressions,
              aggregateExpressions,
              rewrittenResultExpressions,
              planLater(child))

          case _ => Nil
        }
      }

      object CounterStrategy extends Strategy {
        def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
          case Counter(source, child) =>
            CounterExec(source, planLater(child)) :: Nil
          case _ => Nil
        }
      }
    }

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private var operatorId = 0

  /** Locates save/restore pairs surrounding aggregation. */
  val state = new Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StateStoreSaveExec(keys, None,
             UnaryExecNode(agg,
               StateStoreRestoreExec(keys2, None, child))) =>
        val stateId = OperatorStateId(checkpointLocation, operatorId, currentBatchId - 1)
        operatorId += 1

        StateStoreSaveExec(
          keys,
          Some(stateId),
          agg.withNewChildren(
            StateStoreRestoreExec(
              keys,
              Some(stateId),
              child) :: Nil))
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = state +: super.preparations

  /** No need assert supported, as this check has already been done */
  override def assertSupported(): Unit = { }
}
