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

package org.apache.spark.sql.streaming

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.MapGroupsWithState
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.streaming.{KeyedStateImpl, MapGroupsWithStateExec, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreId, StoreUpdate}
import org.apache.spark.sql.streaming.MapGroupsWithStateSuite.SingleKeyStateStore
import org.apache.spark.sql.types.{DataType, IntegerType}

/** Class to check custom state types */
case class RunningCount(count: Long)

class MapGroupsWithStateSuite extends StateStoreMetricsTest with BeforeAndAfterAll {

  import testImplicits._
  import KeyedStateImpl._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("KeyedState - get, exists, update, remove") {
    var state: KeyedStateImpl[String] = null

    def testState(
        expectedData: Option[String],
        shouldBeUpdated: Boolean = false,
        shouldBeRemoved: Boolean = false): Unit = {
      if (expectedData.isDefined) {
        assert(state.exists)
        assert(state.get === expectedData.get)
      } else {
        assert(!state.exists)
        intercept[NoSuchElementException] {
          state.get
        }
      }
      assert(state.getOption === expectedData)
      assert(state.isUpdated === shouldBeUpdated)
      assert(state.isRemoved === shouldBeRemoved)
    }

    // Updating empty state
    state = new KeyedStateImpl[String](None)
    testState(None)
    state.update("")
    testState(Some(""), shouldBeUpdated = true)

    // Updating exiting state
    state = new KeyedStateImpl[String](Some("2"))
    testState(Some("2"))
    state.update("3")
    testState(Some("3"), shouldBeUpdated = true)

    // Removing state
    state.remove()
    testState(None, shouldBeRemoved = true, shouldBeUpdated = false)
    state.remove()      // should be still callable
    state.update("4")
    testState(Some("4"), shouldBeRemoved = false, shouldBeUpdated = true)

    // Updating by null throw exception
    intercept[IllegalArgumentException] {
      state.update(null)
    }
  }

  test("KeyedState - isTimingOut, setTimeoutDuration") {
    import KeyedStateImpl._
    var state: KeyedStateImpl[String] = null
    state = new KeyedStateImpl[String](None)

    assert(state.isTimingOut === false)
    assert(state.getTimeoutTimestamp === TIMEOUT_TIMESTAMP_NOT_SET)
    intercept[UnsupportedOperationException] {
      state.setTimeoutDuration(1000)
    }
    intercept[UnsupportedOperationException] {
      state.setTimeoutDuration("1 day")
    }
    assert(state.getTimeoutTimestamp === TIMEOUT_TIMESTAMP_NOT_SET)

    state = new KeyedStateImpl[String](None, 1000, isTimeoutEnabled = true, isTimingOut = false)
    assert(state.isTimingOut === false)
    assert(state.getTimeoutTimestamp === TIMEOUT_TIMESTAMP_NOT_SET)
    state.setTimeoutDuration(1000)
    assert(state.getTimeoutTimestamp === 2000)
    state.setTimeoutDuration("2 second")
    assert(state.getTimeoutTimestamp === 3000)
    assert(state.isTimingOut === false)

    state = new KeyedStateImpl[String](None, 1000, isTimeoutEnabled = true, isTimingOut = true)
    assert(state.isTimingOut === true)
  }

  test("KeyedState - primitive type") {
    var intState = new KeyedStateImpl[Int](None)
    intercept[NoSuchElementException] {
      intState.get
    }
    assert(intState.getOption === None)

    intState = new KeyedStateImpl[Int](Some(10))
    assert(intState.get == 10)
    intState.update(0)
    assert(intState.get == 0)
    intState.remove()
    intercept[NoSuchElementException] {
      intState.get
    }
  }


  val currentTimestamp = 1000
  val beforeCurrentTimestamp = 999
  val afterCurrentTimestamp = 1001

  // Tests for StateStoreUpdater.updateStateForKeysWithData() when timeout is disabled
  for (priorState <- Seq(None, Some(0))) {
    val priorStateStr = if (priorState.nonEmpty) "prior state set" else "no prior state"
    val testName = s"timeout disabled - $priorStateStr - "

    testStateUpdateWithData(
      testName + "no update",
      stateUpdates = state => { /* do nothing */ },
      timeoutType = KeyedStateTimeout.none,
      priorState = priorState,
      expectedState = priorState)    // should not change

    testStateUpdateWithData(
      testName + "state updated",
      stateUpdates = state => { state.update(5) },
      timeoutType = KeyedStateTimeout.none,
      priorState = priorState,
      expectedState = Some(5))     // should change

    testStateUpdateWithData(
      testName + "state removed",
      stateUpdates = state => { state.remove() },
      timeoutType = KeyedStateTimeout.none,
      priorState = priorState,
      expectedState = None)        // should be removed
  }

  // Tests for StateStoreUpdater.updateStateForKeysWithData() when timeout is enabled
  for (priorState <- Seq(None, Some(0))) {
    for (priorTimeoutTimestamp <- Seq(TIMEOUT_TIMESTAMP_NOT_SET, 1000)) {
      val priorStateStr = if (priorState.nonEmpty) "prior state set" else "no prior state"
      val priorTimeoutStr =
        if (priorTimeoutTimestamp == 1000) "prior timeout set" else "no prior timeout"
      val testName = s"timeout enabled - $priorStateStr, $priorTimeoutStr - "

      testStateUpdateWithData(
        testName + "no update",
        stateUpdates = state => { /* do nothing */ },
        timeoutType = KeyedStateTimeout.withProcessingTime,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = priorState,                           // state should not change
        expectedTimeoutTimestamp = TIMEOUT_TIMESTAMP_NOT_SET) // timestamp should be reset

      testStateUpdateWithData(
        testName + "state updated",
        stateUpdates = state => { state.update(5) },
        timeoutType = KeyedStateTimeout.withProcessingTime,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = Some(5),                              // state should change
        expectedTimeoutTimestamp = TIMEOUT_TIMESTAMP_NOT_SET) // timestamp should be reset

      testStateUpdateWithData(
        testName + "state removed",
        stateUpdates = state => { state.remove() },
        timeoutType = KeyedStateTimeout.withProcessingTime,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = None)                                 // state should be removed

      testStateUpdateWithData(
        testName + "timeout updated",
        stateUpdates = state => { state.setTimeoutDuration(5000) },
        timeoutType = KeyedStateTimeout.withProcessingTime,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = priorState,                           // state should not change
        expectedTimeoutTimestamp = currentTimestamp + 5000)   // timestamp should change

      testStateUpdateWithData(
        testName + "timeout and state updated",
        stateUpdates = state => { state.update(5); state.setTimeoutDuration(5000) },
        timeoutType = KeyedStateTimeout.withProcessingTime,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = Some(5),                              // state should change
        expectedTimeoutTimestamp = currentTimestamp + 5000)   // timestamp should change

      testStateUpdateWithData(
        testName + "timeout updated after state removed",
        stateUpdates = state => { state.remove() ; state.setTimeoutDuration(5000) },
        timeoutType = KeyedStateTimeout.withProcessingTime,
        priorState = priorState,
        priorTimeoutTimestamp = priorTimeoutTimestamp,
        expectedState = None)                                 // state should be removed
    }
  }


  // Tests for StateStoreUpdater.updateStateForTimedOutKeys()
  val preTimeoutState = Some(5)

  testStateUpdateWithTimeout(
    "should not timeout",
    stateUpdates = state => { assert(false, "function called without timeout") },
    priorTimeoutTimestamp = afterCurrentTimestamp,
    expectedState = preTimeoutState,                          // state should not change
    expectedTimeoutTimestamp = afterCurrentTimestamp)         // timestamp should not change

  testStateUpdateWithTimeout(
    "should timeout - no update/remove",
    stateUpdates = state => { /* do nothing */ },
    priorTimeoutTimestamp = beforeCurrentTimestamp,
    expectedState = preTimeoutState,                          // state should not change
    expectedTimeoutTimestamp = TIMEOUT_TIMESTAMP_NOT_SET)     // timestamp should be reset

  testStateUpdateWithTimeout(
    "should timeout - update state",
    stateUpdates = state => { state.update(5) },
    priorTimeoutTimestamp = beforeCurrentTimestamp,
    expectedState = Some(5),                                  // state should change
    expectedTimeoutTimestamp = TIMEOUT_TIMESTAMP_NOT_SET)     // timestamp should be reset

  testStateUpdateWithTimeout(
    "should timeout - remove state",
    stateUpdates = state => { state.remove() },
    priorTimeoutTimestamp = beforeCurrentTimestamp,
    expectedState = None,                                     // state should be removed
    expectedTimeoutTimestamp = TIMEOUT_TIMESTAMP_NOT_SET)

  testStateUpdateWithTimeout(
    "should timeout - timeout updated",
    stateUpdates = state => { state.setTimeoutDuration(2000) },
    priorTimeoutTimestamp = beforeCurrentTimestamp,
    expectedState = preTimeoutState,                          // state should not change
    expectedTimeoutTimestamp = currentTimestamp + 2000)       // timestamp should change

  testStateUpdateWithTimeout(
    "should timeout - timeout and state updated",
    stateUpdates = state => { state.update(5); state.setTimeoutDuration(2000) },
    priorTimeoutTimestamp = beforeCurrentTimestamp,
    expectedState = Some(5),                                  // state should change
    expectedTimeoutTimestamp = currentTimestamp + 2000)       // timestamp should change

  testStateUpdateWithTimeout(
    "should timeout - timeout updated after state removed",
    stateUpdates = state => { state.remove(); state.setTimeoutDuration(2000) },
    priorTimeoutTimestamp = beforeCurrentTimestamp,
    expectedState = None)                                     // state should be removed


  test("flatMapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator.empty
      } else {
        state.update(RunningCount(count))
        Iterator((key, count.toString))
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(stateFunc) // State: Int, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch(("b", "2")),
      assertNumStateRows(total = 1, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("flatMapGroupsWithState - streaming + func returns iterator that updates state lazily") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    // Additionally, it updates state lazily as the returned iterator get consumed
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      values.flatMap { _ =>
        val count = state.getOption.map(_.count).getOrElse(0L) + 1
        if (count == 3) {
          state.remove()
          None
        } else {
          state.update(RunningCount(count))
          Some((key, count.toString))
        }
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(stateFunc) // State: Int, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a", "a", "b"),
      CheckLastBatch(("a", "1"), ("a", "2"), ("b", "1")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch(("b", "2")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1"))
    )
  }

  test("flatMapGroupsWithState - batch") {
    // Function that returns running count only if its even, otherwise does not return
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      Iterator((key, values.size))
    }
    checkAnswer(
      Seq("a", "a", "b").toDS.groupByKey(x => x).flatMapGroupsWithState(stateFunc).toDF,
      Seq(("a", 2), ("b", 1)).toDF)
  }

  test("mapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(RunningCount(count))
        (key, count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(total = 1, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1
      CheckLastBatch(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("mapGroupsWithState - streaming + processing time timeout") {
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (state.isTimingOut) {
        state.remove()
        (key, "-1")
      } else {
        val count = state.getOption.map(_.count).getOrElse(0L) + values.size
        state.update(RunningCount(count))
        state.setTimeoutDuration("10 seconds")
        (key, count.toString)
      }
    }

    val clock = new StreamManualClock
    val inputData = MemoryStream[String]
    val timeout = KeyedStateTimeout.withProcessingTime
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc, timeout) // Types = State: MyState, Out: (Str, Str)

    testStream(result, Append)(
      StartStream(ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, "a"),
      AdvanceManualClock(1 * 1000),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),

      // "a" should not timeout after 1 sec
      AddData(inputData, "b"),
      AdvanceManualClock(1 * 1000),
      CheckLastBatch(("b", "1")),
      assertNumStateRows(total = 2, updated = 1),

      // "a" should timeout after 10 sec
      AddData(inputData, "b"),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(total = 1, updated = 2),

      StopStream,
      AssertOnQuery { q =>
        StateStore.stop()   // dropping in-mem stores to make sure it gets recovered from checkpoint
        true
      },
      StartStream(ProcessingTime("1 second"), triggerClock = clock),

      AddData(inputData, "c"),
      AdvanceManualClock(20 * 1000),
      CheckLastBatch(("b", "-1"), ("c", "1")),
      assertNumStateRows(total = 1, updated = 2),

      AddData(inputData, "c"),
      AdvanceManualClock(20 * 1000),
      CheckLastBatch(("c", "2")),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("mapGroupsWithState - streaming + aggregation") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(RunningCount(count))
        (key, count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)
        .groupByKey(_._1)
        .count()

    testStream(result, Complete)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", 1)),
      AddData(inputData, "a", "b"),
      // mapGroups generates ("a", "2"), ("b", "1"); so increases counts of a and b by 1
      CheckLastBatch(("a", 2), ("b", 1)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"),
      // mapGroups should remove state for "a" and generate ("a", "-1"), ("b", "2") ;
      // so increment a and b by 1
      CheckLastBatch(("a", 3), ("b", 2)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"),
      // mapGroups should recreate state for "a" and generate ("a", "1"), ("c", "1") ;
      // so increment a and c by 1
      CheckLastBatch(("a", 4), ("b", 2), ("c", 1))
    )
  }

  test("mapGroupsWithState - batch") {
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      (key, values.size)
    }

    checkAnswer(
      spark.createDataset(Seq("a", "a", "b"))
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc)
        .toDF,
      spark.createDataset(Seq(("a", 2), ("b", 1))).toDF)
  }

  testQuietly("StateStore.abort on task failure handling") {
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (MapGroupsWithStateSuite.failInTask) throw new Exception("expected failure")
      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      state.update(RunningCount(count))
      (key, count)
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    def setFailInTask(value: Boolean): AssertOnQuery = AssertOnQuery { q =>
      MapGroupsWithStateSuite.failInTask = value
      true
    }

    testStream(result, Append)(
      setFailInTask(false),
      AddData(inputData, "a"),
      CheckLastBatch(("a", 1L)),
      AddData(inputData, "a"),
      CheckLastBatch(("a", 2L)),
      setFailInTask(true),
      AddData(inputData, "a"),
      ExpectFailure[SparkException](),   // task should fail but should not increment count
      setFailInTask(false),
      StartStream(),
      CheckLastBatch(("a", 3L))     // task should not fail, and should show correct count
    )
  }

  def testStateUpdateWithData(
      testName: String,
      stateUpdates: KeyedState[Int] => Unit,
      timeoutType: KeyedStateTimeout = KeyedStateTimeout.none,
      priorState: Option[Int],
      priorTimeoutTimestamp: Long = TIMEOUT_TIMESTAMP_NOT_SET,
      expectedState: Option[Int] = None,
      expectedTimeoutTimestamp: Long = TIMEOUT_TIMESTAMP_NOT_SET): Unit = {

    test(s"StateStoreUpdater - updates with data - $testName") {
      val mapGroupsFunc = (key: Int, values: Iterator[Int], state: KeyedState[Int]) => {
        assert(state.isTimingOut === false, "isTimingOut not false")
        assert(values.nonEmpty, "Some value is expected")
        stateUpdates(state)
        Iterator.empty
      }
      testStateUpdate(
        testTimeoutUpdates = false, mapGroupsFunc, timeoutType,
        priorState, priorTimeoutTimestamp, expectedState, expectedTimeoutTimestamp)
    }
  }

  def testStateUpdateWithTimeout(
      testName: String,
      stateUpdates: KeyedState[Int] => Unit,
      priorTimeoutTimestamp: Long,
      expectedState: Option[Int],
      expectedTimeoutTimestamp: Long = TIMEOUT_TIMESTAMP_NOT_SET): Unit = {

    test(s"StateStoreUpdater - updates for timeout - $testName") {
      val mapGroupsFunc = (key: Int, values: Iterator[Int], state: KeyedState[Int]) => {
        assert(state.isTimingOut === true, "isTimingOut not true")
        assert(values.isEmpty, "values not empty")
        stateUpdates(state)
        Iterator.empty
      }
      testStateUpdate(
        testTimeoutUpdates = true, mapGroupsFunc, KeyedStateTimeout.withProcessingTime,
        preTimeoutState, priorTimeoutTimestamp,
        expectedState, expectedTimeoutTimestamp)
    }
  }

  def testStateUpdate(
      testTimeoutUpdates: Boolean,
      mapGroupsFunc: (Int, Iterator[Int], KeyedState[Int]) => Iterator[Int],
      timeoutType: KeyedStateTimeout,
      priorState: Option[Int],
      priorTimeoutTimestamp: Long,
      expectedState: Option[Int],
      expectedTimeoutTimestamp: Long): Unit = {

    val store = newStore()

    val result = MemoryStream[Int]
      .toDS
      .groupByKey(x => x)
      .flatMapGroupsWithState[Int, Int](mapGroupsFunc, timeout = timeoutType)
    val mapGroupsSparkPlan = result.logicalPlan.collectFirst {
      case MapGroupsWithState(f, k, v, g, d, o, s, t, _) =>
        MapGroupsWithStateExec(f, k, v, g, d, o, None, s, t, currentTimestamp,
          RDDScanExec(g, null, "rdd"))
    }.get

    val updater = new mapGroupsSparkPlan.StateStoreUpdater(store)

    // Prepare store with prior state configs
    if (priorState.nonEmpty || priorTimeoutTimestamp != TIMEOUT_TIMESTAMP_NOT_SET) {
      val row = priorState.map(updater.getUpdatedStateRow).getOrElse(updater.createNewStateRow())
      updater.setTimeoutTimestamp(row, priorTimeoutTimestamp)
      store.put(row.copy())
    }

    val returnedIter = if (testTimeoutUpdates) {
      updater.updateStateForTimedOutKeys()
    } else {
      updater.updateStateForKeysWithData(Iterator(singleKeyForStore))
    }
    returnedIter.size // consumer the iterator to force state updates

    // Verify updated state in store
    val updatedStateRow = store.get
    assert(
      updater.getStateObj(updatedStateRow).map(_.toString.toInt) === expectedState,
      "final state not as expected")
    if (updatedStateRow.nonEmpty) {
      assert(
        updater.getTimeoutTimestamp(updatedStateRow.get) === expectedTimeoutTimestamp,
        "final timeout timestamp not as expected")
    }
  }

  val singleKeyForStore = {
    val intProj = UnsafeProjection.create(Array[DataType](IntegerType))
    intProj.apply(new GenericInternalRow(Array[Any](0))).copy()
  }

  def newStore(): SingleKeyStateStore = new SingleKeyStateStore(singleKeyForStore)
}

object MapGroupsWithStateSuite {

  class SingleKeyStateStore(singleKey: UnsafeRow) extends StateStore() {
    @volatile private var value: UnsafeRow = null
    def get: Option[UnsafeRow] = Option(value)
    def put(newValue: UnsafeRow): Unit = { value = newValue }

    override def iterator(): Iterator[(UnsafeRow, UnsafeRow)] = {
      Option(value).map { v => (singleKey, value) }.iterator
    }
    override def filter(c: (UnsafeRow, UnsafeRow) => Boolean): Iterator[(UnsafeRow, UnsafeRow)] = {
      iterator.filter { case (k, v) => c(k, v) }
    }

    override def get(key: UnsafeRow): Option[UnsafeRow] = checkKey(key) { Option(value) }
    override def put(key: UnsafeRow, newValue: UnsafeRow): Unit = checkKey(key) { value = newValue }
    override def remove(key: UnsafeRow): Unit = checkKey(key) { value = null }
    override def remove(condition: (UnsafeRow) => Boolean): Unit = throwException
    override def commit(): Long = version + 1
    override def abort(): Unit = { }
    override def id: StateStoreId = null
    override def version: Long = 0
    override def updates(): Iterator[StoreUpdate] = throwException
    override def numKeys(): Long = Option(value).size
    override def hasCommitted: Boolean = true
    private def throwException: Nothing = { throw new UnsupportedOperationException }
    private def checkKey[T](key: UnsafeRow)(body: => T): T = {
      require(key.hashCode == singleKey.hashCode, s"$key != $singleKey"); body
    }
  }

  var failInTask = true
}
