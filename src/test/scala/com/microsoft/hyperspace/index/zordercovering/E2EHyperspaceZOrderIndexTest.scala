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

package com.microsoft.hyperspace.index.zordercovering

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig, TestUtils}
import com.microsoft.hyperspace.index.{Content, FileIdTracker, HyperspaceSuite, IndexConstants}
import com.microsoft.hyperspace.index.IndexConstants.{REFRESH_MODE_INCREMENTAL, REFRESH_MODE_QUICK}
import com.microsoft.hyperspace.index.covering.CoveringIndexConfig

class E2EHyperspaceZOrderIndexTest extends QueryTest with HyperspaceSuite {
  private val testDir = inTempDir("e2eTests")
  private val nonPartitionedDataPath = testDir + "/sampleparquet"
  private val partitionedDataPath = testDir + "/samplepartitionedparquet"
  private val fileSystem = new Path(nonPartitionedDataPath).getFileSystem(new Configuration)
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    hyperspace = new Hyperspace(spark)
    fileSystem.delete(new Path(testDir), true)

    val dataColumns = Seq("c1", "c2", "c3", "c4", "c5")
    // save test data non-partitioned.
    SampleData.save(spark, nonPartitionedDataPath, dataColumns)

    // save test data partitioned.
    SampleData.save(spark, partitionedDataPath, dataColumns, Some(Seq("c1", "c3")))
  }

  before {
    // Clear index cache so a new test does not see stale indexes from previous ones.
    clearCache()
  }

  override def afterAll(): Unit = {
    fileSystem.delete(new Path(testDir), true)
    super.afterAll()
  }

  after {
    fileSystem.delete(systemPath, true)
    spark.disableHyperspace()
  }

  test(
    "E2E test for filter query on partitioned and non-partitioned data with and without " +
      "lineage.") {
    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      Seq(true, false).foreach { enableLineage =>
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
          withIndex("filterZOrderIndex") {
            val df = spark.read.parquet(loc)
            val indexConfig =
              ZOrderCoveringIndexConfig("filterZOrderIndex", Seq("c2", "c3"), Seq("c1"))
            hyperspace.createIndex(df, indexConfig)

            {
              // Check z-order index is applied for the second indexed column.
              def query(): DataFrame = df.filter("c3 == 'facebook'").select("c3", "c1")
              verifyIndexUsage(query, indexConfig.indexName)
            }
            {
              def query(): DataFrame =
                df.filter("c2 >= '810a20a2baa24ff3ad493bfbf064569a'").select("c3", "c1")
              verifyIndexUsage(query, indexConfig.indexName)
            }
          }
        }
      }
    }
  }

  test("E2E test for case insensitive filter query utilizing indexes.") {
    val df = spark.read.parquet(nonPartitionedDataPath)
    val indexConfig = ZOrderCoveringIndexConfig("filterIndex", Seq("C3"), Seq("C1"))
    hyperspace.createIndex(df, indexConfig)
    def query(): DataFrame = df.filter("C3 == 'facebook'").select("C3", "c1")
    // Verify if case-insensitive index works with case-insensitive query.
    verifyIndexUsage(query, indexConfig.indexName)
  }

  test("E2E test for case sensitive filter query where changing conf changes behavior.") {
    val df = spark.read.parquet(nonPartitionedDataPath)
    val indexConfig = ZOrderCoveringIndexConfig("filterIndex", Seq("c3"), Seq("c1"))

    hyperspace.createIndex(df, indexConfig)
    def query(): DataFrame = df.filter("C3 == 'facebook'").select("C3", "c1")

    withSQLConf("spark.sql.caseSensitive" -> "true") {
      intercept[AnalysisException] {
        query().show
      }
    }

    withSQLConf("spark.sql.caseSensitive" -> "false") {
      verifyIndexUsage(query, indexConfig.indexName)
    }
  }

  test(
    "E2E test for filter query when all columns are selected on partitioned and " +
      "non-partitioned data with and without lineage.") {
    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      Seq(true, false).foreach { enableLineage =>
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
          withIndex("filterIndex") {
            val df = spark.read.parquet(loc)
            val indexConfig =
              ZOrderCoveringIndexConfig("filterIndex", Seq("c4", "c3"), Seq("c1", "c2", "c5"))

            hyperspace.createIndex(df, indexConfig)
            df.createOrReplaceTempView("t")

            def query(): DataFrame = spark.sql("SELECT * from t where c4 = 1")

            // Verify no Project node is present in the query plan, as a result of using SELECT *
            assert(query().queryExecution.optimizedPlan.collect { case p: Project => p }.isEmpty)

            verifyIndexUsage(query, indexConfig.indexName)
          }
        }
      }
    }
  }

  test(
    "Verify ZOrderFilterRule utilizes indexes correctly after incremental refresh (append-only).") {
    withTempPathAsString { testPath =>
      // Setup. Create data.
      val indexConfig = ZOrderCoveringIndexConfig("index", Seq("c2", "c3", "c4"), Seq("c1"))
      import spark.implicits._
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(10)
        .write
        .parquet(testPath)

      val df = spark.read.load(testPath)
      hyperspace.createIndex(df, indexConfig)
      def query(): DataFrame =
        spark.read.parquet(testPath).filter("c3 == 'facebook'").select("c3", "c1")
      verifyIndexUsage(query, indexConfig.indexName)

      // Append to original data.
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)

      // Check index is not applied because of appended data.
      verifyIndexNotUsed(query)

      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
      verifyIndexUsage(query, indexConfig.indexName)
    }
  }

  test("Validate index usage after incremental refresh with some source data file deleted.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        // Save a copy of source data files.
        val dataColumns = Seq("c1", "c2", "c3", "c4", "c5")
        SampleData.save(spark, testPath, dataColumns)

        val df = spark.read.parquet(testPath)
        val indexConfig = ZOrderCoveringIndexConfig("filterIndex", Seq("c2", "c3"), Seq("c1"))
        hyperspace.createIndex(df, indexConfig)

        // Verify index usage.
        def query(): DataFrame =
          spark.read.parquet(testPath).filter("c3 == 'facebook'").select("c3", "c1")
        verifyIndexUsage(query, indexConfig.indexName)

        // Delete some source data file.
        TestUtils.deleteFiles(testPath, "*parquet", 1)

        // Verify index is not used.
        verifyIndexNotUsed(query)

        // Refresh the index to remove deleted source data file records from index.
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

        // Verify index usage on latest version of index (v=1) after refresh.
        verifyIndexUsage(query, indexConfig.indexName)
      }
    }
  }

  test(
    "Verify ZOrderFilterIndexRule utilizes indexes correctly after incremental refresh " +
      "when some file gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        import spark.implicits._
        val indexConfig = ZOrderCoveringIndexConfig("index", Seq("c5", "c3"), Seq("c4", "c1"))
        SampleData.testData
          .toDF("c1", "c2", "c3", "c4", "c5")
          .limit(10)
          .write
          .parquet(testPath)
        val df = spark.read.load(testPath)

        // Create index.
        hyperspace.createIndex(df, indexConfig)

        // Delete some source data file.
        TestUtils.deleteFiles(testPath, "*parquet", 1)

        // Append to original data.
        SampleData.testData
          .toDF("c1", "c2", "c3", "c4", "c5")
          .limit(3)
          .write
          .mode("append")
          .parquet(testPath)

        def query(): DataFrame =
          spark.read.parquet(testPath).filter("c3 >= 'facebook'").select("c3", "c1")
        verifyIndexNotUsed(query)

        // Refresh index.
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

        // Verify indexes are used, and all index files are picked.
        verifyIndexUsage(query, indexConfig.indexName)

        // Verify correctness of results.
        spark.disableHyperspace()
        val dfWithHyperspaceDisabled = query()
        spark.enableHyperspace()
        val dfWithHyperspaceEnabled = query()
        checkAnswer(dfWithHyperspaceDisabled, dfWithHyperspaceEnabled)
      }
    }
  }

  test(
    "Verify ZOrderFilterIndexRule utilizes indexes correctly after quick refresh" +
      "when some file gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      val indexConfig = ZOrderCoveringIndexConfig("index", Seq("c3", "c4"), Seq("c1", "c2"))
      import spark.implicits._
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.load(testPath)
      val oldFiles = df.inputFiles

      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        // Create index.
        hyperspace.createIndex(df, indexConfig)
      }

      // Delete some source data file.
      TestUtils.deleteFiles(testPath, "*parquet", 1)

      // Append to original data.
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)

      def query(): DataFrame =
        spark.read
          .parquet(testPath)
          .filter("c3 >= 'facebook' and c2 >= '2018-09-03'")
          .select("c3", "c2")

      verifyIndexNotUsed(query)

      // Refresh index.
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_QUICK)
      verifyIndexUsage(query, indexConfig.indexName)

      {
        val df = spark.read.parquet(testPath)
        val appendedFiles = df.inputFiles.diff(oldFiles).map(new Path(_))

        // Verify indexes are used, and all index files are picked.
        verifyRootPaths(query, getIndexFilesPath(indexConfig.indexName, Seq(0)) ++ appendedFiles)

        // Verify correctness of results.
        spark.disableHyperspace()
        val dfWithHyperspaceDisabled = query()
        spark.enableHyperspace()
        val dfWithHyperspaceEnabled = query()
        checkAnswer(dfWithHyperspaceDisabled, dfWithHyperspaceEnabled)
      }

      // Append to original data again.
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(1)
        .write
        .mode("append")
        .parquet(testPath)

      // Refreshed index as quick mode won't be applied with additional appended files.
      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        verifyIndexNotUsed(query)
      }

      // Refreshed index as quick mode can be applied with Hybrid Scan config.
      withSQLConf(TestConfig.HybridScanEnabled: _*) {
        verifyIndexUsage(query, indexConfig.indexName)
      }
    }
  }

  test("Verify ZOrderFilterIndexRule is prior to FilterIndexRule.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        import spark.implicits._
        val zOrderIndexConfig =
          ZOrderCoveringIndexConfig("zindex", Seq("c3"), Seq("c4", "c1"))
        val coveringIndexConfig = CoveringIndexConfig("cindex", Seq("c3"), Seq("c4", "c1"))
        SampleData.testData
          .toDF("c1", "c2", "c3", "c4", "c5")
          .limit(10)
          .write
          .parquet(testPath)
        val df = spark.read.load(testPath)

        // Create indexes.
        hyperspace.createIndex(df, zOrderIndexConfig)
        hyperspace.createIndex(df, coveringIndexConfig)

        def query(): DataFrame =
          spark.read.parquet(testPath).filter("c3 >= 'facebook'").select("c3", "c1")
        verifyIndexUsage(query, zOrderIndexConfig.indexName)
      }
    }
  }

  /**
   * Verify that the query plan has the expected rootPaths.
   *
   * @param optimizedPlan the optimized query plan.
   * @param expectedPaths the expected paths in the query plan.
   */
  private def verifyQueryPlanHasExpectedRootPaths(
      optimizedPlan: LogicalPlan,
      expectedPaths: Seq[Path]): Unit = {
    assert(getAllRootPaths(optimizedPlan).sortBy(_.getName) === expectedPaths.sortBy(_.getName))
  }

  /**
   * Get all rootPaths from a query plan.
   *
   * @param optimizedPlan the optimized query plan.
   * @return a sequence of [[Path]].
   */
  private def getAllRootPaths(optimizedPlan: LogicalPlan): Seq[Path] = {
    optimizedPlan.collect {
      case LogicalRelation(
            HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _),
            _,
            _,
            _) =>
        location.rootPaths
    }.flatten
  }

  private def getIndexFilesPath(indexName: String, versions: Seq[Int] = Seq(0)): Seq[Path] = {
    versions.flatMap { v =>
      Content
        .fromDirectory(
          new Path(systemPath, s"$indexName/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=$v"),
          new FileIdTracker,
          new Configuration)
        .files
    }
  }

  /**
   * Gets the sorted rows from the given dataframe to make it easy to compare with
   * other dataframe.
   *
   * @param df dataframe to collect rows from.
   * @return sorted rows.
   */
  private def getSortedRows(df: DataFrame): Array[Row] = {
    df.orderBy(df.columns.head, df.columns.tail: _*).collect()
  }

  private def verifyIndexNotUsed(f: () => DataFrame) = {
    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = f()
    val planStr = dfWithHyperspaceEnabled.queryExecution.optimizedPlan.toString
    assert(!planStr.contains("Hyperspace("))
  }

  private def verifyIndexUsage(f: () => DataFrame, expectedIndexName: String): Unit = {
    spark.disableHyperspace()
    val dfWithHyperspaceDisabled = f()
    val schemaWithHyperspaceDisabled = dfWithHyperspaceDisabled.schema
    val sortedRowsWithHyperspaceDisabled = getSortedRows(dfWithHyperspaceDisabled)

    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = f()

    val planStr = dfWithHyperspaceEnabled.queryExecution.optimizedPlan.toString
    assert(planStr.contains(s"Hyperspace(Type: ZCI, Name: $expectedIndexName"))

    assert(schemaWithHyperspaceDisabled.equals(dfWithHyperspaceEnabled.schema))
    assert(sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfWithHyperspaceEnabled)))
  }

  private def verifyRootPaths(f: () => DataFrame, expectedRootPaths: Seq[Path]): Unit = {
    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = f()

    verifyQueryPlanHasExpectedRootPaths(
      dfWithHyperspaceEnabled.queryExecution.optimizedPlan,
      expectedRootPaths)
  }

  private def getOriginalQueryPlan(query: DataFrame => DataFrame, df: DataFrame): LogicalPlan = {
    spark.disableHyperspace()
    val p = query(df).queryExecution.optimizedPlan
    spark.enableHyperspace()
    p
  }

  private def equalsRef(a: Set[FileIndex], b: Set[FileIndex]): Boolean = {
    a.size == b.size && a.zip(b).forall(f => f._1 eq f._2)
  }

  private def getFsLocation(plan: LogicalPlan): Set[FileIndex] = {
    plan.collect {
      case LogicalRelation(HadoopFsRelation(loc, _, _, _, _, _), _, _, _) =>
        loc
    }.toSet
  }
}
