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
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData}
import com.microsoft.hyperspace.index.rules.{FilterIndexRule, JoinIndexRule}
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

class E2EHyperspaceRulesTests extends HyperspaceSuite with SQLHelper {
  private val testDir = "src/test/resources/e2eTests/"
  private val nonPartitionedDataPath = testDir + "sampleparquet"
  private val partitionedDataPath = testDir + "samplepartitionedparquet"
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/indexLocation")
  private val fileSystem =
    new Path(nonPartitionedDataPath).getFileSystem(new Configuration)
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

    assert(
      !spark.sessionState.experimentalMethods.extraOptimizations
        .containsSlice(expectedOptimizationRuleBatch))

    spark.enableHyperspace()
    assert(
      spark.sessionState.experimentalMethods.extraOptimizations
        .containsSlice(expectedOptimizationRuleBatch))

    spark.disableHyperspace()
    assert(
      !spark.sessionState.experimentalMethods.extraOptimizations
        .containsSlice(expectedOptimizationRuleBatch))
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

    assert(
      queryPlanHasExpectedRootPaths(
        dfWithHyperspaceEnabled.queryExecution.optimizedPlan,
        getIndexFilesPath(indexConfig.indexName)))

    assert(schemaWithHyperspaceDisabled.equals(dfWithHyperspaceEnabled.schema))
    assert(sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfWithHyperspaceEnabled)))

    spark.disableHyperspace()
    val dfAfterHyperspaceDisabled = query(df)

    assert(
      queryPlanHasExpectedRootPaths(
        dfAfterHyperspaceDisabled.queryExecution.optimizedPlan,
        rootPathsWithHyperspaceDisabled))

    assert(schemaWithHyperspaceDisabled.equals(dfAfterHyperspaceDisabled.schema))
    assert(
      sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfAfterHyperspaceDisabled)))
  }

  test("Verify Join Index Rule utilizes indexes correctly after incremental refresh.") {
    // Setup. Create Data.
    val sampleParquetDataLocation = "src/test/resources/sampleparquet"
    spark.conf.set(IndexConstants.REFRESH_APPEND_ENABLED, true)
    val refreshTestLocation = sampleParquetDataLocation + "refresh"
    FileUtils.delete(new Path(refreshTestLocation))
    val indexConfig = IndexConfig(s"index", Seq("RGUID"), Seq("imprs"))
    import spark.implicits._
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .limit(10)
      .write
      .parquet(refreshTestLocation)
    val df = spark.read.load(refreshTestLocation)

    // Create Index.
    hyperspace.createIndex(df, indexConfig)

    // Append to Original Data.
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .limit(3)
      .write
      .mode("append")
      .parquet(refreshTestLocation)

    // Refresh Index.
    hyperspace.refreshIndex(indexConfig.indexName)

    // Create a Join Query
    val leftDf = spark.read.parquet(refreshTestLocation)
    val rightDf = spark.read.parquet(refreshTestLocation)
    def query(): DataFrame = {
      leftDf
        .join(rightDf, leftDf("RGUID") === rightDf("RGUID"))
        .select(leftDf("RGUID"), rightDf("imprs"))
    }

    // Verify Indexes are used, and all index files are picked.
    verifyIndexUsage(
      query,
      getIndexFilesPath(indexConfig.indexName) ++
        getIndexFilesPath(indexConfig.indexName))

    // Verify Bucketing works as expected. This is reflected in shuffle nodes being eliminated
    // when hyperspace is enabled.
    spark.disableHyperspace()
    val dfWithHyperspaceDisabled = query()
    var shuffleNodes = dfWithHyperspaceDisabled.queryExecution.executedPlan.collect {
      case s : ShuffleExchangeExec => s
    }
    assert(shuffleNodes.size == 2)

    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = query()
    shuffleNodes = dfWithHyperspaceEnabled.queryExecution.executedPlan.collect {
      case s : ShuffleExchangeExec => s
    }
    assert(shuffleNodes.isEmpty)

    FileUtils.delete(new Path(refreshTestLocation))
  }

  test("Test for isHyperspaceEnabled().") {
    assert(!spark.isHyperspaceEnabled(), "Hyperspace must be disabled by default.")
    spark.enableHyperspace()
    assert(spark.isHyperspaceEnabled())
    spark.disableHyperspace()
    assert(!spark.isHyperspaceEnabled())
  }

  /**
   * Check that if the query plan has the expected rootPaths.
   *
   * @param optimizedPlan the optimized query plan.
   * @param expectedPaths the expected paths in the query plan.
   * @return has or not.
   */
  private def queryPlanHasExpectedRootPaths(
      optimizedPlan: LogicalPlan,
      expectedPaths: Seq[Path]): Boolean = {
    getAllRootPaths(optimizedPlan).equals(expectedPaths)
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

  private def getIndexFilesPath(indexName: String): Seq[Path] = {
    var files = Content
      .fromDirectory(
        new Path(systemPath, s"$indexName/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0"))
      .files

    // Append refreshed files to the list if such a directory exists.
    val v1Dir = new Path(
      systemPath, s"$indexName/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")
    if (fileSystem.exists(v1Dir)) {
      files ++= Content
        .fromDirectory(
          new Path(systemPath, s"$indexName/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1"))
        .files
    }

    files
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

    assert(
      queryPlanHasExpectedRootPaths(
        dfWithHyperspaceEnabled.queryExecution.optimizedPlan,
        expectedRootPaths))

    assert(schemaWithHyperspaceDisabled.equals(dfWithHyperspaceEnabled.schema))
    assert(sortedRowsWithHyperspaceDisabled.sameElements(getSortedRows(dfWithHyperspaceEnabled)))
  }
}
