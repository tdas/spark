package org.apache.spark.sql.streaming

/**
 * Created by tdas on 9/24/15.
 */
package object execution {
  type Strategy = org.apache.spark.sql.catalyst.planning.GenericStrategy[StreamingPlan]
}
