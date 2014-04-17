
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

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.util.IntParam

object ClusteringDemoModel {

  def main(args: Array[String]) {
    if (args.size < 5) {
      println("Usage: ClusteringDemoModel <master> <tweets file> <# clusters> <# iterations> <model file>")
      System.exit(1)
    }

    val Array(master, tweetsFile, IntParam(numClusters), IntParam(numIterations), modelFile) = args
    
    val sc = new SparkContext(master, "ClusteringDemoModel",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    // Cluster the data
    val tweets = sc.textFile(tweetsFile, 100)
    val statuses = tweets.map(line => line.split('\t')(5))
    val vectors = statuses.map(featurize).persist
    val model = KMeans.train(vectors, numClusters, numIterations)
    println("Learnt model")
    
    // Save the model to a file
    sc.makeRDD(model.clusterCenters, 10).saveAsObjectFile(modelFile)
    println("Saved model as object file to " + modelFile)

    // Apply the model to predict the clusters
    val clustered = statuses.map(t => (t, model.predict(featurize(t))))
    clustered.take(100).foreach(println)
    println("Done")
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

