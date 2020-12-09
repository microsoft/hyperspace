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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestUtils}
import com.microsoft.hyperspace.index.IndexConstants.{GLOBBING_PATTERN_KEY, REFRESH_MODE_INCREMENTAL, REFRESH_MODE_QUICK}
import com.microsoft.hyperspace.index.execution.BucketUnionStrategy
import com.microsoft.hyperspace.index.rules.{FilterIndexRule, JoinIndexRule}
import com.microsoft.hyperspace.util.PathUtils

class E2EHyperspaceRulesTests extends QueryTest with HyperspaceSuite {
  private val testDir = "src/test/resources/e2eTests/"
  private val nonPartitionedDataPath = testDir + "sampleparquet"
  private val partitionedDataPath = testDir + "samplepartitionedparquet"
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/indexLocation")
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

  test("verify enableHyperspace()/disableHyperspace() plug in/out optimization rules.") {
    val expectedOptimizationRuleBatch = Seq(JoinIndexRule, FilterIndexRule)
    val expectedOptimizationStrategy = Seq(BucketUnionStrategy)

    assert(
      !spark.sessionState.experimentalMethods.extraOptimizations
        .containsSlice(expectedOptimizationRuleBatch))
    assert(
      !spark.sessionState.experimentalMethods.extraStrategies
        .containsSlice(expectedOptimizationStrategy))

    spark.enableHyperspace()
    assert(
      spark.sessionState.experimentalMethods.extraOptimizations
        .containsSlice(expectedOptimizationRuleBatch))
    assert(
      spark.sessionState.experimentalMethods.extraStrategies
        .containsSlice(expectedOptimizationStrategy))

    spark.disableHyperspace()
    assert(
      !spark.sessionState.experimentalMethods.extraOptimizations
        .containsSlice(expectedOptimizationRuleBatch))
    assert(
      !spark.sessionState.experimentalMethods.extraStrategies
        .containsSlice(expectedOptimizationStrategy))
  }

  test(
    "E2E test for filter query on partitioned and non-partitioned data with and without " +
      "lineage.") {
    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      Seq(true, false).foreach { enableLineage =>
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
          withIndex("filterIndex") {
            val df = spark.read.parquet(loc)
            val indexConfig = IndexConfig("filterIndex", Seq("c3"), Seq("c1"))

            hyperspace.createIndex(df, indexConfig)

            def query(): DataFrame = df.filter("c3 == 'facebook'").select("c3", "c1")

            verifyIndexUsage(query, getIndexFilesPath(indexConfig.indexName))
          }
        }
      }
    }
  }

  test("E2E test for case insensitive filter query utilizing indexes.") {
    val df = spark.read.parquet(nonPartitionedDataPath)
    val indexConfig = IndexConfig("filterIndex", Seq("C3"), Seq("C1"))

    hyperspace.createIndex(df, indexConfig)

    def query(): DataFrame = df.filter("C3 == 'facebook'").select("C3", "c1")

    // Verify if case-insensitive index works with case-insensitive query.
    verifyIndexUsage(query, getIndexFilesPath(indexConfig.indexName))
  }

  test("E2E test for case sensitive filter query where changing conf changes behavior.") {
    val df = spark.read.parquet(nonPartitionedDataPath)
    val indexConfig = IndexConfig("filterIndex", Seq("c3"), Seq("c1"))

    hyperspace.createIndex(df, indexConfig)
    def query(): DataFrame = df.filter("C3 == 'facebook'").select("C3", "c1")

    withSQLConf("spark.sql.caseSensitive" -> "true") {
      intercept[AnalysisException] {
        query().show
      }
    }

    withSQLConf("spark.sql.caseSensitive" -> "false") {
      verifyIndexUsage(query, getIndexFilesPath(indexConfig.indexName))
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
            val indexConfig = IndexConfig("filterIndex", Seq("c4", "c3"), Seq("c1", "c2", "c5"))

            hyperspace.createIndex(df, indexConfig)
            df.createOrReplaceTempView("t")

            def query(): DataFrame = spark.sql("SELECT * from t where c4 = 1")

            // Verify no Project node is present in the query plan, as a result of using SELECT *
            assert(query().queryExecution.optimizedPlan.collect { case p: Project => p }.isEmpty)

            verifyIndexUsage(query, getIndexFilesPath(indexConfig.indexName))
          }
        }
      }
    }
  }

  test(
    "E2E test for join query on partitioned and non-partitioned data with and without lineage.") {
    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      Seq(true, false).foreach { enableLineage =>
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
          withIndex("leftIndex", "rightIndex") {
            val leftDf = spark.read.parquet(loc)
            val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))

            hyperspace.createIndex(leftDf, leftDfIndexConfig)

            val rightDf = spark.read.parquet(loc)
            val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))
            hyperspace.createIndex(rightDf, rightDfIndexConfig)

            def query(): DataFrame = {
              leftDf
                .join(rightDf, leftDf("c3") === rightDf("c3"))
                .select(leftDf("c1"), rightDf("c4"))
            }

            verifyIndexUsage(
              query,
              getIndexFilesPath(leftDfIndexConfig.indexName) ++
                getIndexFilesPath(rightDfIndexConfig.indexName))
          }
        }
      }
    }
  }

  test("E2E test for join query with case-insensitive column names.") {
    val leftDf = spark.read.parquet(nonPartitionedDataPath)
    val leftDfIndexConfig = IndexConfig("leftIndex", Seq("C3"), Seq("c1"))
    hyperspace.createIndex(leftDf, leftDfIndexConfig)

    val rightDf = spark.read.parquet(nonPartitionedDataPath)
    val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("C4"))
    hyperspace.createIndex(rightDf, rightDfIndexConfig)

    def query(): DataFrame = {
      leftDf.join(rightDf, leftDf("c3") === rightDf("C3")).select(leftDf("C1"), rightDf("c4"))
    }

    verifyIndexUsage(
      query,
      getIndexFilesPath(leftDfIndexConfig.indexName) ++
        getIndexFilesPath(rightDfIndexConfig.indexName))
  }

  test("E2E test for join query with alias columns is not supported.") {
    def verifyNoChange(f: () => DataFrame): Unit = {
      spark.disableHyperspace()
      val originalPlan = f().queryExecution.optimizedPlan
      val updatedPlan = JoinIndexRule(originalPlan)
      assert(originalPlan.equals(updatedPlan))
    }

    withView("t1", "t2") {
      val leftDf = spark.read.parquet(nonPartitionedDataPath)
      val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))
      hyperspace.createIndex(leftDf, leftDfIndexConfig)

      val rightDf = spark.read.parquet(nonPartitionedDataPath)
      val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))
      hyperspace.createIndex(rightDf, rightDfIndexConfig)

      leftDf.createOrReplaceTempView("t1")
      rightDf.createOrReplaceTempView("t2")

      // Test: join query with alias columns in join condition is not optimized.
      def query1(): DataFrame = spark.sql("""SELECT alias, c4
          |from t2, (select c3 as alias, c1 from t1)
          |where t2.c3 = alias""".stripMargin)
      verifyNoChange(query1)

      // Test: join query with alias columns in project columns is not optimized.
      def query2(): DataFrame = spark.sql("""SELECT alias, c4
          |from t2, (select c3, c1 as alias from t1) as newt
          |where t2.c3 = newt.c3""".stripMargin)
      verifyNoChange(query2)
    }
  }

  test("E2E test for join query on catalog temp tables/views") {
    withView("t1", "t2") {
      val leftDf = spark.read.parquet(nonPartitionedDataPath)
      val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))
      hyperspace.createIndex(leftDf, leftDfIndexConfig)

      val rightDf = spark.read.parquet(nonPartitionedDataPath)
      val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))
      hyperspace.createIndex(rightDf, rightDfIndexConfig)

      // Test whether indexes work with catalog tables or not
      leftDf.createOrReplaceTempView("t1")
      rightDf.createOrReplaceTempView("t2")

      def query(): DataFrame = spark.sql("SELECT t1.c1, t2.c4 FROM t1, t2 WHERE t1.c3 = t2.c3")

      verifyIndexUsage(
        query,
        getIndexFilesPath(leftDfIndexConfig.indexName) ++
          getIndexFilesPath(rightDfIndexConfig.indexName))
    }
  }

  ignore("E2E test for join query on external catalog tables") {
    // TODO: Ignoring this test as it depends on hive for testing.
    withTable("t1", "t2") {
      // save tables on hive metastore as external tables
      spark.sql(s"""
          |CREATE EXTERNAL TABLE t1
          |(c1 string, c3 string)
          |STORED AS PARQUET
          |LOCATION '$nonPartitionedDataPath'
        """.stripMargin)
      spark.sql(s"""
          |CREATE EXTERNAL TABLE t2
          |(c3 string, c4 int)
          |STORED AS PARQUET
          |LOCATION '$nonPartitionedDataPath'
        """.stripMargin)

      val leftDf = spark.table("t1")
      val rightDf = spark.table("t2")

      val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))
      val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))

      hyperspace.createIndex(leftDf, leftDfIndexConfig)
      hyperspace.createIndex(rightDf, rightDfIndexConfig)

      // Test whether indexes work with catalog tables or not
      def query(): DataFrame = spark.sql("SELECT t1.c1, t2.c4 FROM t1, t2 WHERE t1.c3 = t2.c3")
      verifyIndexUsage(
        query,
        getIndexFilesPath(leftDfIndexConfig.indexName) ++
          getIndexFilesPath(rightDfIndexConfig.indexName))
    }
  }

  test("E2E test for join query on managed catalog tables") {
    withTable("t1", "t2") {
      val table1Location = testDir + "tables/t1"
      val table2Location = testDir + "tables/t2"
      val originalDf = spark.read.parquet(nonPartitionedDataPath)
      originalDf.select("c1", "c3").write.option("path", table1Location).saveAsTable("t1")
      originalDf.select("c3", "c4").write.option("path", table2Location).saveAsTable("t2")

      val leftDf = spark.table("t1")
      val rightDf = spark.table("t2")
      val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))
      val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))
      hyperspace.createIndex(leftDf, leftDfIndexConfig)
      hyperspace.createIndex(rightDf, rightDfIndexConfig)

      // Test whether indexes work with catalog tables or not
      def query(): DataFrame = spark.sql("SELECT t1.c1, t2.c4 FROM t1, t2 WHERE t1.c3 = t2.c3")
      verifyIndexUsage(
        query,
        getIndexFilesPath(leftDfIndexConfig.indexName) ++
          getIndexFilesPath(rightDfIndexConfig.indexName))
    }
  }

  test("E2E test for join query with two child sub-query as both filter query.") {
    val leftDf = spark.read.parquet(nonPartitionedDataPath)
    val leftDfJoinIndexConfig = IndexConfig("leftJoinIndex", Seq("c3"), Seq("c4"))
    hyperspace.createIndex(leftDf, leftDfJoinIndexConfig)

    val leftDfFilterIndexConfig = IndexConfig("leftDfFilterIndex", Seq("c4"), Seq("c3"))
    hyperspace.createIndex(leftDf, leftDfFilterIndexConfig)

    val rightDf = spark.read.parquet(nonPartitionedDataPath)
    val rightDfJoinIndexConfig = IndexConfig("rightDfJoinIndex", Seq("c3"), Seq("c5"))

    hyperspace.createIndex(rightDf, rightDfJoinIndexConfig)
    val rightDfFilterIndexConfig =
      IndexConfig("rightDfFilterIndex", Seq("c5"), Seq("c3"))

    hyperspace.createIndex(rightDf, rightDfFilterIndexConfig)

    def query(left: DataFrame, right: DataFrame): () => DataFrame = { () =>
      left
        .filter("c4 == 2")
        .select("c4", "c3")
        .join(right.filter("c5 == 3000").select("c5", "c3"), left("c3") === right("c3"))
        .select(left("c3"), left("c4"), right("c5"))
    }

    verifyIndexUsage(
      query(leftDf, rightDf),
      getIndexFilesPath(leftDfJoinIndexConfig.indexName) ++
        getIndexFilesPath(rightDfJoinIndexConfig.indexName))
  }

  test("E2E test for first enableHyperspace() followed by disableHyperspace().") {
    val df = spark.read.parquet(nonPartitionedDataPath)
    val indexConfig = IndexConfig("filterIndex", Seq("c3"), Seq("c1"))
    hyperspace.createIndex(df, indexConfig)

    def query(df: DataFrame): DataFrame = {
      df.filter("c3 == 'facebook'").select("c3", "c1")
    }

    val dfWithHyperspaceDisabled = query(df)
    val rootPathsWithHyperspaceDisabled =
      getAllRootPaths(dfWithHyperspaceDisabled.queryExecution.optimizedPlan)
    val schemaWithHyperspaceDisabled = dfWithHyperspaceDisabled.schema
    val sortedRowsWithHyperspaceDisabled = getSortedRows(dfWithHyperspaceDisabled)

    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = query(df)

    verifyQueryPlanHasExpectedRootPaths(
      dfWithHyperspaceEnabled.queryExecution.optimizedPlan,
      getIndexFilesPath(indexConfig.indexName))

    assert(schemaWithHyperspaceDisabled.equals(dfWithHyperspaceEnabled.schema))
    assert(sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfWithHyperspaceEnabled)))

    spark.disableHyperspace()
    val dfAfterHyperspaceDisabled = query(df)

    verifyQueryPlanHasExpectedRootPaths(
      dfAfterHyperspaceDisabled.queryExecution.optimizedPlan,
      rootPathsWithHyperspaceDisabled)

    assert(schemaWithHyperspaceDisabled.equals(dfAfterHyperspaceDisabled.schema))
    assert(
      sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfAfterHyperspaceDisabled)))
  }

  test("Verify JoinIndexRule utilizes indexes correctly after incremental refresh (append-only).") {
    withTempPathAsString { testPath =>
      // Setup. Create data.
      val indexConfig = IndexConfig("index", Seq("c2"), Seq("c4"))
      import spark.implicits._
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.load(testPath)

      // Create index.
      hyperspace.createIndex(df, indexConfig)

      // Append to original data.
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)

      // Refresh index.
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

      // Create a join query.
      val leftDf = spark.read.parquet(testPath)
      val rightDf = spark.read.parquet(testPath)

      def query(): DataFrame = {
        leftDf
          .join(rightDf, leftDf("c2") === rightDf("c2"))
          .select(leftDf("c2"), rightDf("c4"))
      }

      // Verify indexes are used, and all index files are picked.
      verifyIndexUsage(
        query,
        getIndexFilesPath(indexConfig.indexName, Seq(0, 1)) ++
          getIndexFilesPath(indexConfig.indexName, Seq(0, 1)))

      // With Hyperspace disabled, verify there are shuffle and sort nodes as expected.
      spark.disableHyperspace()
      val dfWithHyperspaceDisabled = query()
      var shuffleNodes = dfWithHyperspaceDisabled.queryExecution.executedPlan.collect {
        case s: ShuffleExchangeExec => s
      }
      assert(shuffleNodes.size == 2)
      var sortNodes = dfWithHyperspaceDisabled.queryExecution.executedPlan.collect {
        case s: SortExec => s
      }
      assert(sortNodes.size == 2)

      // With Hyperspace enabled, verify bucketing works as expected. This is reflected in
      // shuffle nodes being eliminated.
      spark.enableHyperspace()
      val dfWithHyperspaceEnabled = query()
      shuffleNodes = dfWithHyperspaceEnabled.queryExecution.executedPlan.collect {
        case s: ShuffleExchangeExec => s
      }
      assert(shuffleNodes.isEmpty)

      // SortExec is expected to be present because there are multiple files per bucket.
      sortNodes = dfWithHyperspaceEnabled.queryExecution.executedPlan.collect {
        case s: SortExec => s
      }
      assert(sortNodes.size == 2)
    }
  }

  test("Test for isHyperspaceEnabled().") {
    assert(!spark.isHyperspaceEnabled(), "Hyperspace must be disabled by default.")
    spark.enableHyperspace()
    assert(spark.isHyperspaceEnabled())
    spark.disableHyperspace()
    assert(!spark.isHyperspaceEnabled())
  }

  test("Validate index usage after incremental refresh with some source data file deleted.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        // Save a copy of source data files.
        val dataColumns = Seq("c1", "c2", "c3", "c4", "c5")
        SampleData.save(spark, testPath, dataColumns)

        // Create index on original source data files.
        val df = spark.read.parquet(testPath)
        val indexConfig = IndexConfig("filterIndex", Seq("c3"), Seq("c1"))
        hyperspace.createIndex(df, indexConfig)

        // Verify index usage for index version (v=0).
        def query(): DataFrame =
          spark.read.parquet(testPath).filter("c3 == 'facebook'").select("c3", "c1")

        verifyIndexUsage(query, getIndexFilesPath(indexConfig.indexName))

        // Delete some source data file.
        TestUtils.deleteFiles(testPath, "*parquet", 1)

        // Verify index is not used.
        spark.enableHyperspace()
        val planRootPaths = getAllRootPaths(query().queryExecution.optimizedPlan)
        spark.disableHyperspace()
        assert(planRootPaths.equals(Seq(PathUtils.makeAbsolute(testPath))))

        // Refresh the index to remove deleted source data file records from index.
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

        // Verify index usage on latest version of index (v=1) after refresh.
        verifyIndexUsage(query, getIndexFilesPath(indexConfig.indexName, Seq(1)))
      }
    }
  }

  test(
    "Verify JoinIndexRule utilizes indexes correctly after incremental refresh when some file " +
      "gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        // Setup. Create data.
        val indexConfig = IndexConfig("index", Seq("c2"), Seq("c4"))
        import spark.implicits._
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

        // Refresh index.
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

        // Create a join query.
        val leftDf = spark.read.parquet(testPath)
        val rightDf = spark.read.parquet(testPath)

        def query(): DataFrame = {
          leftDf
            .join(rightDf, leftDf("c2") === rightDf("c2"))
            .select(leftDf("c2"), rightDf("c4"))
        }

        // Verify indexes are used, and all index files are picked.
        verifyIndexUsage(
          query,
          getIndexFilesPath(indexConfig.indexName, Seq(1)) ++ // for Left
            getIndexFilesPath(indexConfig.indexName, Seq(1))) // for Right

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
    "Verify JoinIndexRule utilizes indexes correctly after quick refresh when some file " +
      "gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      // Setup. Create data.
      val indexConfig = IndexConfig("index", Seq("c2"), Seq("c4"))
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

      // Refresh index.
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_QUICK)

      {
        // Create a join query.
        val leftDf = spark.read.parquet(testPath)
        val rightDf = spark.read.parquet(testPath)

        def query(): DataFrame = {
          leftDf
            .join(rightDf, leftDf("c2") === rightDf("c2"))
            .select(leftDf("c2"), rightDf("c4"))
        }

        val appendedFiles = leftDf.inputFiles.diff(oldFiles).map(new Path(_))

        // Verify indexes are used, and all index files are picked.
        verifyIndexUsage(
          query,
          getIndexFilesPath(indexConfig.indexName, Seq(0)) ++ appendedFiles ++
            getIndexFilesPath(indexConfig.indexName, Seq(0)) ++ appendedFiles) // for Right

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

      {
        // Create a join query with updated dataset.
        val leftDf = spark.read.parquet(testPath)
        val rightDf = spark.read.parquet(testPath)

        def query(): DataFrame = {
          leftDf
            .join(rightDf, leftDf("c2") === rightDf("c2"))
            .select(leftDf("c2"), rightDf("c4"))
        }
        // Refreshed index as quick mode won't be applied with additional appended files.
        withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
          spark.disableHyperspace()
          val dfWithHyperspaceDisabled = query()
          val basePlan = dfWithHyperspaceDisabled.queryExecution.optimizedPlan
          spark.enableHyperspace()
          val dfWithHyperspaceEnabled = query()
          assert(basePlan.equals(dfWithHyperspaceEnabled.queryExecution.optimizedPlan))
        }

        // Refreshed index as quick mode can be applied with Hybrid Scan config.
        withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true") {
            spark.disableHyperspace()
            val dfWithHyperspaceDisabled = query()
            val basePlan = dfWithHyperspaceDisabled.queryExecution.optimizedPlan
            spark.enableHyperspace()
            val dfWithHyperspaceEnabled = query()
            assert(!basePlan.equals(dfWithHyperspaceEnabled.queryExecution.optimizedPlan))
            checkAnswer(dfWithHyperspaceDisabled, dfWithHyperspaceEnabled)
          }
        }
      }
    }
  }

  test("Hybrid scan works well with modified data when globbing pattern is used.") {
    withTempPathAsString { testPath =>
      val absoluteTestPath = PathUtils.makeAbsolute(testPath)
      val globPath = absoluteTestPath + "/*"
      val p1 = absoluteTestPath + "/1"
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p1)

      // Create index with globbing pattern.
      val df = spark.read.option(GLOBBING_PATTERN_KEY, globPath).parquet(globPath)
      val indexConfig = IndexConfig("index", Seq("clicks"), Seq("query"))
      hyperspace.createIndex(df, indexConfig)

      // Append data to a new directory which matches the globbing pattern.
      val p2 = absoluteTestPath + "/2"
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(p2)

      val df2 = spark.read.parquet(globPath)
      def filterQuery: DataFrame = df2.filter(df2("clicks") <= 2000).select(df2("query"))
      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan
      spark.enableHyperspace()

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        // Check appended file is added to relation node or not.
        val nodes = planWithHybridScan.collect {
          case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            // Verify old data files are not present but newly appended files are included.
            val p1UriPath = new Path(p1).toUri.getPath
            val p2UriPath = new Path(p2).toUri.getPath
            assert(!fsRelation.location.inputFiles.exists(_.contains(p1UriPath)))
            assert(fsRelation.location.inputFiles.exists(_.contains(p2UriPath)))
            // Verify index data files are present.
            assert(fsRelation.location.inputFiles.exists(_.contains("index")))
            p
        }
        // Filter Index and Parquet format source file can be handled with 1 LogicalRelation.
        assert(nodes.length === 1)
        checkAnswer(baseQuery, filter)
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
          new FileIdTracker)
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

  private def verifyIndexUsage(f: () => DataFrame, expectedRootPaths: Seq[Path]): Unit = {
    spark.disableHyperspace()
    val dfWithHyperspaceDisabled = f()
    val schemaWithHyperspaceDisabled = dfWithHyperspaceDisabled.schema
    val sortedRowsWithHyperspaceDisabled = getSortedRows(dfWithHyperspaceDisabled)

    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = f()

    verifyQueryPlanHasExpectedRootPaths(
      dfWithHyperspaceEnabled.queryExecution.optimizedPlan,
      expectedRootPaths)

    assert(schemaWithHyperspaceDisabled.equals(dfWithHyperspaceEnabled.schema))
    assert(sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfWithHyperspaceEnabled)))
  }
}
