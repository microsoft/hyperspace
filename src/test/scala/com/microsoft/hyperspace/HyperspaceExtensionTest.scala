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
package com.microsoft.hyperspace

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}

import com.microsoft.hyperspace.index.{Content, FileIdTracker, HyperspaceSuite, IndexConfig, IndexConstants}
import com.microsoft.hyperspace.telemetry.Constants.HYPERSPACE_EVENT_LOGGER_CLASS_KEY
import com.microsoft.hyperspace.util.FileUtils

class HyperspaceExtensionTest extends HyperspaceSuite {
  private val sampleDeptDataLocation = inTempDir("dept")
  private val sampleEmpDataLocation = inTempDir("emp")

  private val departments = Seq(
    (10, "Accounting", "New York"),
    (20, "Research", "Dallas"),
    (30, "Sales", "Chicago"),
    (40, "Operations", "Boston"))

  private val employees = Seq(
    (7369, "SMITH", 20),
    (7499, "ALLEN", 30),
    (7521, "WARD", 30),
    (7566, "JONES", 20),
    (7698, "BLAKE", 30),
    (7782, "CLARK", 10),
    (7788, "SCOTT", 20),
    (7839, "KING", 10),
    (7844, "TURNER", 30),
    (7876, "ADAMS", 20),
    (7900, "JAMES", 30),
    (7934, "MILLER", 10),
    (7902, "FORD", 20),
    (7654, "MARTIN", 30))

  override protected lazy val spark: SparkSession = SparkSession
    .builder()
    .master(s"local[$numParallelism]")
    .config(HYPERSPACE_EVENT_LOGGER_CLASS_KEY, "com.microsoft.hyperspace.MockEventLogger")
    .config("delta.log.cacheSize", "3")
    .config("spark.databricks.delta.snapshotPartitions", "2")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
      "spark.sql.extensions",
      "io.delta.sql.DeltaSparkSessionExtension," +
        "com.microsoft.hyperspace.HyperspaceSparkSessionExtension")
    .config("spark.sql.shuffle.partitions", "5")
    .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .appName(suiteName)
    .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sparkSession = spark
    import sparkSession.implicits._
    FileUtils.delete(new Path(sampleDeptDataLocation))
    FileUtils.delete(new Path(sampleEmpDataLocation))

    departments
      .toDF("deptId", "deptName", "location")
      .write
      .mode("overwrite")
      .parquet(sampleDeptDataLocation)

    employees
      .toDF("empId", "empName", "deptId")
      .write
      .mode("overwrite")
      .parquet(sampleEmpDataLocation)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleDeptDataLocation))
    FileUtils.delete(new Path(sampleEmpDataLocation))
    super.beforeAll()
  }

  test("Verify ApplyHyperspace is used with hyperspace extension session") {
    MockEventLogger.reset()

    val deptDF = spark.read.parquet(sampleDeptDataLocation)
    val empDF = spark.read.parquet(sampleEmpDataLocation)

    val deptIndexConfig = IndexConfig("deptIndex", Seq("deptId"), Seq("deptName"))
    val empIndexConfig = IndexConfig("empIndex", Seq("deptId"), Seq("empName"))

    // Create Hyperspace indexes.
    val hs = new Hyperspace(spark)
    hs.createIndex(deptDF, deptIndexConfig)
    hs.createIndex(empDF, empIndexConfig)

    // Make sure new index is available to all.
    assert(Hyperspace.getContext(spark).indexCollectionManager.indexes.count == 2)

    def filterQuery(): DataFrame = deptDF.filter("deptId == '30'").select("deptId", "deptName")

    verifyIndexUsage(filterQuery, getIndexFilesPath(deptIndexConfig.indexName))

    def eqJoinQuery(): DataFrame =
      empDF
        .join(deptDF, empDF("deptId") === deptDF("deptId"))
        .select(empDF("empName"), deptDF("deptName"))

    verifyIndexUsage(
      eqJoinQuery,
      getIndexFilesPath(deptIndexConfig.indexName) ++ getIndexFilesPath(empIndexConfig.indexName))
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
