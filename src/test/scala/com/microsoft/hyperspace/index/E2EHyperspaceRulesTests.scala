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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData}
import com.microsoft.hyperspace.index.rules.{FilterIndexRule, JoinIndexRule}

class E2EHyperspaceRulesTests extends HyperspaceSuite {
  private val sampleData = SampleData.testData
  private val testDir = "src/test/resources/e2eTests/"
  private val sampleParquetDataLocation = testDir + "sampleparquet"
  override val systemPath = new Path(testDir + "indexLocation")
  private val fileSystem = new Path(sampleParquetDataLocation).getFileSystem(new Configuration)
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    import spark.implicits._
    hyperspace = new Hyperspace(spark)

    fileSystem.delete(new Path(sampleParquetDataLocation), true)

    val dfFromSample = sampleData.toDF("c1", "c2", "c3", "c4", "c5")
    dfFromSample.write.parquet(sampleParquetDataLocation)
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

  test("E2E test for filter query.") {
    val df = spark.read.parquet(sampleParquetDataLocation)
    val indexConfig = IndexConfig("filterIndex", Seq("c3"), Seq("c1"))

    hyperspace.createIndex(df, indexConfig)

    def query(): DataFrame = df.filter("c3 == 'facebook'").select("c3", "c1")

    verifyIndexUsage(query, Seq(getIndexFilesPath(indexConfig.indexName)))
  }

  test("E2E test for filter query when all columns are selected.") {
    val df = spark.read.parquet(sampleParquetDataLocation)
    val indexConfig = IndexConfig("filterIndex", Seq("c4", "c3"), Seq("c1", "c2", "c5"))

    hyperspace.createIndex(df, indexConfig)
    df.createOrReplaceTempView("t")

    def query(): DataFrame = spark.sql("SELECT * from t where c4 = 1")

    // Verify no Project node is present in the query plan, as a result of using SELECT *
    assert(query().queryExecution.optimizedPlan.collect { case p: Project => p }.isEmpty, true)

    verifyIndexUsage(query, Seq(getIndexFilesPath(indexConfig.indexName)))
  }

  test("E2E test for join query.") {
    val leftDf = spark.read.parquet(sampleParquetDataLocation)
    val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))

    hyperspace.createIndex(leftDf, leftDfIndexConfig)

    val rightDf = spark.read.parquet(sampleParquetDataLocation)
    val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))
    hyperspace.createIndex(rightDf, rightDfIndexConfig)

    def query(): DataFrame = {
      leftDf.join(rightDf, leftDf("c3") === rightDf("c3")).select(leftDf("c1"), rightDf("c4"))
    }

    verifyIndexUsage(
      query,
      Seq(
        getIndexFilesPath(leftDfIndexConfig.indexName),
        getIndexFilesPath(rightDfIndexConfig.indexName)))
  }

  test("E2E test for join query on catalog temp tables/views") {
    withView("t1", "t2") {
      val leftDf = spark.read.parquet(sampleParquetDataLocation)
      val leftDfIndexConfig = IndexConfig("leftIndex", Seq("c3"), Seq("c1"))
      hyperspace.createIndex(leftDf, leftDfIndexConfig)

      val rightDf = spark.read.parquet(sampleParquetDataLocation)
      val rightDfIndexConfig = IndexConfig("rightIndex", Seq("c3"), Seq("c4"))
      hyperspace.createIndex(rightDf, rightDfIndexConfig)

      // Test whether indexes work with catalog tables or not
      leftDf.createOrReplaceTempView("t1")
      rightDf.createOrReplaceTempView("t2")

      def query(): DataFrame = spark.sql("SELECT t1.c1, t2.c4 FROM t1, t2 WHERE t1.c3 = t2.c3")

      verifyIndexUsage(
        query,
        Seq(
          getIndexFilesPath(leftDfIndexConfig.indexName),
          getIndexFilesPath(rightDfIndexConfig.indexName)))
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
          |LOCATION '$sampleParquetDataLocation'
        """.stripMargin)
      spark.sql(s"""
          |CREATE EXTERNAL TABLE t2
          |(c3 string, c4 int)
          |STORED AS PARQUET
          |LOCATION '$sampleParquetDataLocation'
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
        Seq(
          getIndexFilesPath(leftDfIndexConfig.indexName),
          getIndexFilesPath(rightDfIndexConfig.indexName)))
    }
  }

  test("E2E test for join query on managed catalog tables") {
    withTable("t1", "t2") {
      val table1Location = testDir + "tables/t1"
      val table2Location = testDir + "tables/t2"
      val originalDf = spark.read.parquet(sampleParquetDataLocation)
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
        Seq(
          getIndexFilesPath(leftDfIndexConfig.indexName),
          getIndexFilesPath(rightDfIndexConfig.indexName)))
    }
  }

  test("E2E test for join query with two child sub-query as both filter query.") {
    val leftDf = spark.read.parquet(sampleParquetDataLocation)
    val leftDfJoinIndexConfig = IndexConfig("leftJoinIndex", Seq("c3"), Seq("c4"))
    hyperspace.createIndex(leftDf, leftDfJoinIndexConfig)

    val leftDfFilterIndexConfig = IndexConfig("leftDfFilterIndex", Seq("c4"), Seq("c3"))
    hyperspace.createIndex(leftDf, leftDfFilterIndexConfig)

    val rightDf = spark.read.parquet(sampleParquetDataLocation)
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
      Seq(
        getIndexFilesPath(leftDfJoinIndexConfig.indexName),
        getIndexFilesPath(rightDfJoinIndexConfig.indexName)))
  }

  test("E2E test for first enableHyperspace() followed by disableHyperspace().") {
    val df = spark.read.parquet(sampleParquetDataLocation)
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
        Seq(getIndexFilesPath(indexConfig.indexName))))

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
        location.rootPaths.head
    }
  }

  private def getIndexFilesPath(indexName: String): Path = {
    new Path(systemPath, s"$indexName/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
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
