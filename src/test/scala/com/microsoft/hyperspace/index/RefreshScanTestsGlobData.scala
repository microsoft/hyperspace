/*
 * Copyright (2020) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.index

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{DataFrame, QueryTest}

import com.microsoft.hyperspace.TestUtils.latestIndexLogEntry
import com.microsoft.hyperspace.index.IndexConstants.GLOBBING_PATTERN_KEY
import com.microsoft.hyperspace.util.PathUtils
import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData}

class RefreshScanTestsGlobData extends QueryTest with HyperspaceSuite {
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/indexLocation")

  var originalDf: DataFrame = _
  var partition1: DataFrame = _
  var partition2: DataFrame = _
  var partition3: DataFrame = _
  var partition4: DataFrame = _
  private var hs: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._
    hs = new Hyperspace(spark)
    originalDf = SampleData.testData.toDF("c1", "c2", "c3", "c4", "c5")

    partition1 = originalDf.where("c5 = 1000")
    partition2 = originalDf.where("c5 = 2000")
    partition3 = originalDf.where("c5 = 3000")
    partition4 = originalDf.where("c5 = 4000")
  }

  before {
    // Clear index cache so a new test does not see stale indexes from previous ones.
    clearCache()
  }

  test("RefreshScan indexes eligible files when scan pattern with wildcards is provided.") {
    withTempPathAsString { testPath =>
      withIndex("index") {
        val dataPath = new Path(PathUtils.makeAbsolute(testPath), "data").toString
        val globPath = dataPath + "/*"
        val p1 = dataPath + "/1000"
        val p2 = dataPath + "/2000"

        // Create index.
        partition1.write.parquet(p1)
        val df = spark.read.option(GLOBBING_PATTERN_KEY, globPath).parquet(globPath)
        val indexConfig = IndexConfig("index", Seq("c1"), Seq("c5"))
        hs.createIndex(df, indexConfig)

        // Append data in another location that satisfies globbed path.
        partition2.write.parquet(p2)

        // Refresh index with scan pattern.
        hs.refreshIndex("index", "incremental", Some("data/20*"))

        // Validate index contents.
        val index = latestIndexLogEntry(systemPath, indexConfig.indexName)
        val relation = index.relations.head
        val indexedFiles = relation.data.properties.content.files
        assert(relation.rootPaths.equals(Seq(globPath)))
        assert(indexedFiles.forall(path =>
          path.toString.contains("data/1000") || path.toString.contains("data/2000")))
        assert(indexedFiles.exists(_.toString.contains("data/1000")))
        assert(indexedFiles.exists(_.toString.contains("data/2000")))

        // Validate results.
        val df2 = spark.read.parquet(globPath)
        def query: DataFrame = df2.filter("c1 = '2017-09-03'").select("c1", "c5")
        spark.disableHyperspace()
        val baseQuery = query
        val basePlan = baseQuery.queryExecution.optimizedPlan

        spark.enableHyperspace()
        val queryWithHs = query
        val planWithHs = queryWithHs.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHs))

        val files = planWithHs.collect {
          case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            fsRelation.location.inputFiles
        }.flatten

        // Check data files are replaced by index files.
        assert(files.nonEmpty && files.forall(_.contains("index")))
        checkAnswer(baseQuery, queryWithHs)
      }
    }
  }

  test("RefreshScan indexes doesn't include files not satisfying the scan pattern.") {
    withTempPathAsString { testPath =>
      withIndex("index") {
        val dataPath = new Path(PathUtils.makeAbsolute(testPath), "data").toString
        val globPath = dataPath + "/*"
        val p1 = dataPath + "/1000"
        val p2 = dataPath + "/2000"
        val p3 = dataPath + "/3000"

        // Create index.
        partition1.write.parquet(p1)
        val df = spark.read.option(GLOBBING_PATTERN_KEY, globPath).parquet(globPath)
        val indexConfig = IndexConfig("index", Seq("c1"), Seq("c5"))
        hs.createIndex(df, indexConfig)

        // Append data to two new locations that satisfy globbed path.
        partition2.write.parquet(p2)
        partition3.write.parquet(p3)

        // Refresh index with scan pattern. Note that only one new partition is being indexed.
        hs.refreshIndex("index", "incremental", Some("data/20*"))

        // Validate index contents.
        var index = latestIndexLogEntry(systemPath, indexConfig.indexName)
        var relation = index.relations.head
        var indexedFiles = relation.data.properties.content.files
        assert(relation.rootPaths.equals(Seq(globPath)))
        assert(indexedFiles.forall(path =>
          path.toString.contains("data/1000") || path.toString.contains("data/2000")))
        assert(indexedFiles.exists(_.toString.contains("data/1000")))
        assert(indexedFiles.exists(_.toString.contains("data/2000")))
        assert(!indexedFiles.exists(_.toString.contains("data/3000")))

        // Validate results.
        val df2 = spark.read.parquet(globPath)
        def query: DataFrame = df2.filter("c1 = '2017-09-03'").select("c1", "c5")

        {
          // Hyperspace should not pick index because all data files are not indexed.
          spark.disableHyperspace()
          val baseQuery = query
          val basePlan = baseQuery.queryExecution.optimizedPlan

          spark.enableHyperspace()
          val queryWithHs = query
          val planWithHs = queryWithHs.queryExecution.optimizedPlan
          assert(basePlan.equals(planWithHs))
        }

        // Refresh index for missing directory.
        hs.refreshIndex("index", "incremental", Some("data/30*"))

        // Validate index contents.
        index = latestIndexLogEntry(systemPath, indexConfig.indexName)
        relation = index.relations.head
        indexedFiles = relation.data.properties.content.files
        assert(relation.rootPaths.equals(Seq(globPath)))
        assert(
          indexedFiles.forall(
            path =>
              path.toString.contains("data/1000") ||
                path.toString.contains("data/2000") ||
                path.toString.contains("data/3000")))
        assert(indexedFiles.exists(_.toString.contains("data/1000")))
        assert(indexedFiles.exists(_.toString.contains("data/2000")))
        assert(indexedFiles.exists(_.toString.contains("data/3000")))

        {
          // Hyperspace should pick index because now all data files are indexed.
          spark.disableHyperspace()
          val baseQuery = query
          val basePlan = baseQuery.queryExecution.optimizedPlan

          spark.enableHyperspace()
          val queryWithHs = query
          val planWithHs = queryWithHs.queryExecution.optimizedPlan
          assert(!basePlan.equals(planWithHs))

          val files = planWithHs.collect {
            case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
              fsRelation.location.inputFiles
          }.flatten

          // Check data files are replaced by index files.
          assert(files.nonEmpty && files.forall(_.contains("index")))
          checkAnswer(baseQuery, queryWithHs)
        }
      }
    }
  }
}
