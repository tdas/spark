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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.SharedSQLContext

class FileCatalogSuite extends SharedSQLContext {

  test("ListingFileCatalog: leaf files are qualified paths") {

    withTempDir { dir =>
      val file = new File(dir, "text.txt")
      stringToFile(file, "text")

      val path = new Path(file.getCanonicalPath)
      val catalog = new ListingFileCatalog(sqlContext.sparkSession, Seq(path), Map.empty, None) {
        def leafFilePaths: Seq[Path] = leafFiles.keys.toSeq
        def leafDirPaths: Seq[Path] = leafDirToChildrenFiles.keys.toSeq
      }
      assert(catalog.leafFilePaths.forall(p => p.toString.startsWith("file:/")))
      assert(catalog.leafDirPaths.forall(p => p.toString.startsWith("file:/")))
    }
  }
}
