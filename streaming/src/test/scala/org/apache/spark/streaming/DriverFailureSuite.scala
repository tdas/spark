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

package org.apache.spark.streaming

import java.io.File

import com.google.common.io.Files

import org.apache.spark.{HashPartitioner, Logging}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.util.{DriverFailureTestReceiver, ReceiverBasedDriverFailureTest}
import org.apache.spark.util.Utils

/**
 * This testsuite tests driver failures by explicitly stopping the streaming context at random
 * times while the stream is running using the real clock.
 */
class DriverFailureSuite extends TestSuiteBase with Logging {

  var tempDir: String = null
  val numBatches = 30

  override def batchDuration = Milliseconds(1000)

  override def useManualClock = false

  override def beforeFunction() {
    super.beforeFunction()
    tempDir = Files.createTempDir().toString
  }

  override def afterFunction() {
    super.afterFunction()
    Utils.deleteRecursively(new File(tempDir))
  }

  test("multiple failures with map") {
    MasterFailureTest.testMap(tempDir, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(tempDir, numBatches, batchDuration)
  }

  // TODO (TD): Explain how the test works
  test("multiple failures with receiver and updateStateByKey") {

    // Define the DStream operation to test under driver failures
    val operation = (st: DStream[String]) => {

      val mapPartitionFunc = (iterator: Iterator[String]) => {
        Iterator(iterator.flatMap(_.split(" ")).map(_ -> 1L).reduce((x, y) => (x._1, x._2 + y._2)))
      }

      val updateFunc = (iterator: Iterator[(String, Seq[Long], Option[Seq[Long]])]) => {
        iterator.map { case (key, values, state) =>
          val combined = (state.getOrElse(Seq.empty) ++ values).sorted
          if (state.isEmpty || state.get.max != DriverFailureTestReceiver.maxRecordsPerBlock) {
            val oldState = s"[${ state.map { _.max }.getOrElse(-1) }, ${state.map { _.distinct.sum }.getOrElse(0)}]"
            val newState = s"[${combined.max}, ${combined.distinct.sum}]"
            println(s"Updated state for $key: state = $oldState, new values = $values, new state = $newState")
          }
          (key, combined)
        }
      }

      st.mapPartitions(mapPartitionFunc)
        .updateStateByKey[Seq[Long]](updateFunc, new HashPartitioner(2), rememberPartitioner = false)
        .checkpoint(batchDuration * 5)
    }

    val maxValue = DriverFailureTestReceiver.maxRecordsPerBlock
    val expectedValues = (1L to maxValue).toSet

    // Define the function to verify the output of the DStream operation
    val verify = (time: Time, output: Seq[(String, Seq[Long])]) => {
      val outputStr = output.map { x => (x._1, x._2.distinct.sum) }.sortBy(_._1).mkString(", ")
      println(s"State at $time: $outputStr")

      val incompletelyReceivedWords = output.filter { _._2.max < maxValue }
      if (incompletelyReceivedWords.size > 1) {
        val debugStr = incompletelyReceivedWords.map { x =>
          s"""${x._1}: ${x._2.mkString(",")}, sum = ${x._2.distinct.sum}"""
        }.mkString("\n")
        throw new Exception(s"Incorrect processing of input, all input not processed:\n$debugStr\n")
      }

      output.foreach { case (key, values) =>
        if (!values.forall(expectedValues.contains)) {
          val sum = values.distinct.sum
          val debugStr = values.zip(1L to values.size).map {
            x => if (x._1 == x._2) x._1 else s"[${x._2}]"
          }.mkString(",") + s", sum = $sum"
          throw new Exception(s"Incorrect sequence of values in output:\n$debugStr\n")
        }
      }
    }

    val driverTest = new ReceiverBasedDriverFailureTest(200, 50, operation, verify, tempDir)
    driverTest.testAndGetError().map { errorMessage =>
      fail(errorMessage)
    }
  }
}


