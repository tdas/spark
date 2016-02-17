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

package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends StreamSourceProvider with DataSourceRegister {

  override def shortName(): String = "kinesis"

  override def createSource(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val caseInsensitiveOptions = new CaseInsensitiveMap(parameters)
    val regionName = caseInsensitiveOptions.getOrElse("regionName", {
      throw new IllegalArgumentException("regionName is not specified")
    })
    val endpointUrl = caseInsensitiveOptions.getOrElse("endpointUrl", {
      throw new IllegalArgumentException("endpointUrl is not specified")
    })
    val streamNames = caseInsensitiveOptions.getOrElse("streamNames", {
      throw new IllegalArgumentException("streamNames is not specified")
    }).split(',').toSet
    val initialPosInStream =
      caseInsensitiveOptions.getOrElse("initialPosInStream", "LATEST") match {
        case "LATEST" => InitialPositionInStream.LATEST
        case "TRIM_HORIZON" => InitialPositionInStream.TRIM_HORIZON
        case pos => throw new IllegalArgumentException(s"unknown initialPosInStream: $pos")
      }
    val awsCredentialsOption =
      for (accessKeyId <- caseInsensitiveOptions.get("AWS_ACCESS_KEY_ID");
           secretKey <- caseInsensitiveOptions.get("AWS_SECRET_ACCESS_KEY"))
        yield new SerializableAWSCredentials(accessKeyId, secretKey)

    new KinesisSource(
      sqlContext,
      regionName,
      endpointUrl,
      streamNames,
      initialPosInStream,
      awsCredentialsOption
    )
  }
}
