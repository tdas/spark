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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ObjectOperator
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.types._


object FlatMapGroupsWithStateExecHelper {

  val supportedVersions = Seq(1, 2)

  /**
   * Class to capture deserialized state and timestamp return by the state manager.
   * This is intended for reuse.
   */
  case class StateData(
      var keyRow: UnsafeRow = null,
      var stateRow: UnsafeRow = null,
      var stateObj: Any = null,
      var timeoutTimestamp: Long = -1) {

    private[FlatMapGroupsWithStateExecHelper] def withNew(
        newKeyRow: UnsafeRow,
        newStateRow: UnsafeRow,
        newStateObj: Any,
        newTimeout: Long): this.type = {
      keyRow = newKeyRow
      stateRow = newStateRow
      stateObj = newStateObj
      timeoutTimestamp = newTimeout
      this
    }
  }

  sealed trait StateManager extends Serializable {
    def stateSchema: StructType
    def getState(store: StateStore, keyRow: UnsafeRow): StateData
    def putState(store: StateStore, keyRow: UnsafeRow, state: Any, timeoutTimestamp: Long): Unit
    def removeState(store: StateStore, keyRow: UnsafeRow): Unit
    def getAllState(store: StateStore): Iterator[StateData]
    def version: Int
  }

  def createStateManager(
      stateEncoder: ExpressionEncoder[Any],
      shouldStoreTimestamp: Boolean,
      stateFormatVersion: Int): StateManager = {
    stateFormatVersion match {
      case 1 => new StateManagerImplV1(stateEncoder, shouldStoreTimestamp)
      case 2 => new StateManagerImplV2(stateEncoder, shouldStoreTimestamp)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }

  // ===============================================================================================
  // =========================== Private implementations of StateManager ===========================
  // ===============================================================================================

  private abstract class StateManagerImplBase(val version: Int, shouldStoreTimestamp: Boolean)
    extends StateManager {

    protected def stateSerializerExprs: Seq[Expression]
    protected def stateDeserializerExpr: Expression
    protected def timeoutTimestampOrdinalInRow: Int

    /** Get deserialized state and corresponding timeout timestamp for a key */
    override def getState(store: StateStore, keyRow: UnsafeRow): StateData = {
      val stateRow = store.get(keyRow)
      stateDataForGets.withNew(keyRow, stateRow, getStateObject(stateRow), getTimestamp(stateRow))
    }

    /** Put state and timeout timestamp for a key */
    override def putState(store: StateStore, key: UnsafeRow, state: Any, timestamp: Long): Unit = {
      val stateRow = getStateRow(state)
      setTimestamp(stateRow, timestamp)
      store.put(key, stateRow)
    }

    override def removeState(store: StateStore, keyRow: UnsafeRow): Unit = {
      store.remove(keyRow)
    }

    override def getAllState(store: StateStore): Iterator[StateData] = {
      val stateData = StateData()
      store.getRange(None, None).map { p =>
        stateData.withNew(p.key, p.value, getStateObject(p.value), getTimestamp(p.value))
      }
    }

    private lazy val stateSerializerFunc = ObjectOperator.serializeObjectToRow(stateSerializerExprs)
    private lazy val stateDeserializerFunc = {
      ObjectOperator.deserializeRowToObject(stateDeserializerExpr, stateSchema.toAttributes)
    }
    private lazy val stateDataForGets = StateData()

    protected def getStateObject(row: UnsafeRow): Any = {
      if (row != null) stateDeserializerFunc(row) else null
    }

    protected def getStateRow(obj: Any): UnsafeRow = {
      stateSerializerFunc(obj)
    }

    /** Returns the timeout timestamp of a state row is set */
    private def getTimestamp(stateRow: UnsafeRow): Long = {
      if (shouldStoreTimestamp && stateRow != null) {
        stateRow.getLong(timeoutTimestampOrdinalInRow)
      } else NO_TIMESTAMP
    }

    /** Set the timestamp in a state row */
    private def setTimestamp(stateRow: UnsafeRow, timeoutTimestamps: Long): Unit = {
      if (shouldStoreTimestamp) stateRow.setLong(timeoutTimestampOrdinalInRow, timeoutTimestamps)
    }
  }


  private class StateManagerImplV1(
      stateEncoder: ExpressionEncoder[Any],
      shouldStoreTimestamp: Boolean) extends StateManagerImplBase(1, shouldStoreTimestamp) {

    private val timestampTimeoutAttribute =
      AttributeReference("timeoutTimestamp", dataType = IntegerType, nullable = false)()

    private val stateAttributes: Seq[Attribute] = {
      val encSchemaAttribs = stateEncoder.schema.toAttributes
      if (shouldStoreTimestamp) encSchemaAttribs :+ timestampTimeoutAttribute else encSchemaAttribs
    }

    override val stateSchema: StructType = stateAttributes.toStructType

    override val timeoutTimestampOrdinalInRow: Int = {
      stateAttributes.indexOf(timestampTimeoutAttribute)
    }

    override val stateSerializerExprs: Seq[Expression] = {
      val encoderSerializer = stateEncoder.namedExpressions
      if (shouldStoreTimestamp) {
        encoderSerializer :+ Literal(GroupStateImpl.NO_TIMESTAMP)
      } else {
        encoderSerializer
      }
    }

    override val stateDeserializerExpr: Expression = {
      // Note that this must be done in the driver, as resolving and binding of deserializer
      // expressions to the encoded type can be safely done only in the driver.
      stateEncoder.resolveAndBind().deserializer
    }

    override protected def getStateRow(obj: Any): UnsafeRow = {
      require(obj != null, "State object cannot be null")
      super.getStateRow(obj)
    }
  }


  private class StateManagerImplV2(
      stateEncoder: ExpressionEncoder[Any],
      shouldStoreTimestamp: Boolean) extends StateManagerImplBase(2, shouldStoreTimestamp) {

    /** Schema of the state rows saved in the state store */
    override val stateSchema: StructType = {
      var schema = new StructType().add("groupState", stateEncoder.schema, nullable = true)
      if (shouldStoreTimestamp) schema = schema.add("timeoutTimestamp", LongType, nullable = false)
      schema
    }

    // Ordinals of the information stored in the state row
    private val nestedStateOrdinal = 0
    override val timeoutTimestampOrdinalInRow = 1

    override val stateSerializerExprs: Seq[Expression] = {
      val boundRefToSpecificInternalRow = BoundReference(
        0, stateEncoder.serializer.head.collect { case b: BoundReference => b.dataType }.head, true)

      val nestedStateSerExpr =
        CreateNamedStruct(stateEncoder.namedExpressions.flatMap(e => Seq(Literal(e.name), e)))

      val nullSafeNestedStateSerExpr = {
        val nullLiteral = Literal(null, nestedStateSerExpr.dataType)
        CaseWhen(Seq(IsNull(boundRefToSpecificInternalRow) -> nullLiteral), nestedStateSerExpr)
      }

      if (shouldStoreTimestamp) {
        Seq(nullSafeNestedStateSerExpr, Literal(GroupStateImpl.NO_TIMESTAMP))
      } else {
        Seq(nullSafeNestedStateSerExpr)
      }
    }

    override val stateDeserializerExpr: Expression = {
      // Note that this must be done in the driver, as resolving and binding of deserializer
      // expressions to the encoded type can be safely done only in the driver.
      val boundRefToNestedState =
        BoundReference(nestedStateOrdinal, stateEncoder.schema, nullable = true)
      val deserExpr = stateEncoder.resolveAndBind().deserializer.transformUp {
        case BoundReference(ordinal, _, _) => GetStructField(boundRefToNestedState, ordinal)
      }
      CaseWhen(Seq(IsNull(boundRefToNestedState) -> Literal(null)), elseValue = deserExpr)
    }
  }
}
