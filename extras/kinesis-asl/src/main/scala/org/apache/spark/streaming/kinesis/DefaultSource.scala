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

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.util.{AwsHostNameUtils, HttpUtils}

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

    val streams = caseInsensitiveOptions.getOrElse("streams", {
      throw new IllegalArgumentException("Option 'streams' is not specified")
    }).split(",", -1).toSet
    if (streams.isEmpty || streams.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        "Option 'streams' is invalid. Please use comma separated string (e.g., 'stream1,stream2')")
    }

    val regionOption = caseInsensitiveOptions.get("region")
    val endpointOption = caseInsensitiveOptions.get("endpoint")
    val (region, endpoint) = (regionOption, endpointOption) match {
      case (Some(_region), Some(_endpoint)) =>
        if (RegionUtils.getRegionByEndpoint(_endpoint).getName != _region) {
          throw new IllegalArgumentException(
            s"'region'(${_region}) doesn't match to 'endpoint'(${_endpoint})")
        }
        (_region, _endpoint)
      case (Some(_region), None) =>
        (_region, RegionUtils.getRegion(_region).getServiceEndpoint("kinesis"))
      case (None, Some(_endpoint)) =>
        (RegionUtils.getRegionByEndpoint(_endpoint).getName, _endpoint)
      case (None, None) =>
        throw new IllegalArgumentException("Either 'region' or 'endpoint' should be specified")
    }

    val initialPosInStream =
      caseInsensitiveOptions.getOrElse("initialPosInStream", "LATEST") match {
        case "LATEST" => InitialPositionInStream.LATEST
        case "TRIM_HORIZON" => InitialPositionInStream.TRIM_HORIZON
        case pos =>
          throw new IllegalArgumentException(s"Unknown value of option initialPosInStream: $pos")
      }

    val accessKeyOption = caseInsensitiveOptions.get("accessKey")
    val secretKeyOption = caseInsensitiveOptions.get("secretKey")
    val credentials = (accessKeyOption, secretKeyOption) match {
      case (Some(accessKey), Some(secretKey)) =>
        new SerializableAWSCredentials(accessKey, secretKey)
      case (Some(accessKey), None) =>
        throw new IllegalArgumentException(
          s"'accessKey' is set but 'secretKey' is not found")
      case (None, Some(secretKey)) =>
        throw new IllegalArgumentException(
          s"'secretKey' is set but 'accessKey' is not found")
      case (None, None) =>
        SerializableAWSCredentials(new DefaultAWSCredentialsProviderChain().getCredentials())
    }

    new KinesisSource(
      sqlContext,
      region,
      endpoint,
      streams,
      initialPosInStream,
      credentials)
  }
}
