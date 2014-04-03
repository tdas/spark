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

package org.apache.spark.ui.env

import org.apache.spark.scheduler._
import org.apache.spark.ui._

private[ui] class EnvironmentTab(parent: SparkUI) extends UITab("environment") {
  val appName = parent.appName
  val basePath = parent.basePath

  def start() {
    listener = Some(new EnvironmentListener)
    attachPage(new IndexPage(this))
  }

  def environmentListener: EnvironmentListener = {
    assert(listener.isDefined, "EnvironmentTab has not started yet!")
    listener.get.asInstanceOf[EnvironmentListener]
  }
}

/**
 * A SparkListener that prepares information to be displayed on the EnvironmentTab
 */
private[ui] class EnvironmentListener extends SparkListener {
  var jvmInformation = Seq[(String, String)]()
  var sparkProperties = Seq[(String, String)]()
  var systemProperties = Seq[(String, String)]()
  var classpathEntries = Seq[(String, String)]()

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      val environmentDetails = environmentUpdate.environmentDetails
      jvmInformation = environmentDetails("JVM Information")
      sparkProperties = environmentDetails("Spark Properties")
      systemProperties = environmentDetails("System Properties")
      classpathEntries = environmentDetails("Classpath Entries")
    }
  }
}
