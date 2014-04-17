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

package org.apache.spark.streaming.examples.demo

import TwitterHelper._

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.util.IntParam

object ClusteringDemo {

  def main(args: Array[String]) {
    
    //Process program arguments and set properties
    if (args.length < 3) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <master>  <model file>  <cluster number>")
      System.exit(1)
    }

    val Array(master, modelFile, IntParam(clusterNumber)) = args

    //Initialize SparkStreaming context
    val conf = new SparkConf().setMaster(master).setAppName(this.getClass.getSimpleName)
      .setSparkHome(System.getenv("SPARK_HOME")).setJars(Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val ssc = new StreamingContext(conf, Seconds(1))

    // Read the model from file
    println("Reading model")
    val model = new KMeansModel(
      ssc.sparkContext.objectFile[Array[Double]](modelFile).collect.map(Vectors.dense)
    )
    println("Read model")
    // Apply model to filter tweets
    val tweets = TwitterUtils.createStream(ssc, Some(authorizations(0)))
    val statuses = tweets.map(_.getText)
    val filteredTweets = statuses.filter(t => model.predict(featurize(t)) == clusterNumber)
    filteredTweets.print()

    // Start the streaming computation
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
   * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a model while still
   * getting excellent accuracy (otherwise every pair of Unicode characters would
   * potentially be a feature).
   */
  def featurize(s: String): Vector = {
    val lower = s.toLowerCase.filter(c => c.isLetter || c.isSpaceChar)
    val result = new Array[Double](1000)
    val bigrams = lower.sliding(2).toArray
    for (h <- bigrams.map(_.hashCode % 1000)) {
      result(h) += 1.0 / bigrams.length
    }
    Vectors.dense(result)
  }
}
