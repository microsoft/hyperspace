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

package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.mockito.Mockito._

import com.microsoft.hyperspace.{HyperspaceException, SampleData}
import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.sources.FileBasedSourceProviderManager
import com.microsoft.hyperspace.util.FileUtils

class CreateActionTest extends HyperspaceSuite with SQLHelper {
  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = inTempDir("sampleparquet")
  private val indexConfig = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private var df: DataFrame = _

  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])

  private val sparkSession = spark
  object CreateActionBaseWrapper extends CreateActionBase(mockDataManager) {
    override def spark: SparkSession = sparkSession
    def getSourceRelations(df: DataFrame): Seq[Relation] = {
      val provider = new FileBasedSourceProviderManager(spark)
      df.queryExecution.optimizedPlan.collect {
        case l: LeafNode if provider.isSupportedRelation(l) =>
          provider.getRelation(l).createRelationMetadata(fileIdTracker)
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    when(mockLogManager.getLatestLog()).thenReturn(None)
    when(mockLogManager.getLatestId()).thenReturn(None)

    import spark.implicits._
    FileUtils.delete(new Path(sampleParquetDataLocation))

    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)

    df = spark.read.parquet(sampleParquetDataLocation)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  test("validate passes for valid index config and df") {
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() fails if df is not logical plan") {
    import spark.implicits._
    val df = Seq((1, 11), (2, 22), (3, 33)).toDF("c1", "c2")
    val indexConfig = IndexConfig("name", Seq("c1"), Seq("c2"))

    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(
      ex.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported. " +
          "Source plan: LocalTableScan "))
  }

  test("validate() fails if index config doesn't contain columns from df") {
    val indexConfig = IndexConfig("name", Seq("c1"), Seq("c2"))
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(ex.getMessage.contains("Index config is not applicable to dataframe schema"))
  }

  test("validate() passes if no earlier index logs are found") {
    when(mockLogManager.getLatestLog()).thenReturn(None)
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() passes if old index logs are found with DOESNOTEXIST state") {
    when(mockLogManager.getLatestLog()).thenReturn(Some(TestLogEntry(DOESNOTEXIST)))
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() fails if old index logs found with non-DOESNOTEXIST state") {
    when(mockLogManager.getLatestLog()).thenReturn(Some(TestLogEntry(ACTIVE)))
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(
      ex.getMessage.contains(s"Another Index with name ${indexConfig.indexName} already exists"))
  }

  test("op() fails if indexed column is of wrong case and spark is case-sensitive") {
    when(mockLogManager.getLatestLog()).thenReturn(Some(TestLogEntry(ACTIVE)))
    val indexConfig = IndexConfig("index1", Seq("rgUID"), Seq("Date"))
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      val ex = intercept[HyperspaceException](action.op())
      assert(
        ex.getMessage.contains(
          "Columns 'rgUID' could not be resolved from available " +
            "source columns:\n" +
            "root\n " +
            "|-- Date: string (nullable = true)\n " +
            "|-- RGUID: string (nullable = true)\n " +
            "|-- Query: string (nullable = true)\n " +
            "|-- imprs: integer (nullable = true)\n " +
            "|-- clicks: integer (nullable = true)"))
    }
  }

  test("op() fails if included config is of wrong case and spark is case-sensitive") {
    when(mockLogManager.getLatestLog()).thenReturn(Some(TestLogEntry(ACTIVE)))
    val indexConfig = IndexConfig("index1", Seq("RGUID"), Seq("dATE"))
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      val ex = intercept[HyperspaceException](action.op())
      assert(
        ex.getMessage.contains(
          "Columns 'dATE' could not be resolved from available " +
            "source columns:\n" +
            "root\n " +
            "|-- Date: string (nullable = true)\n " +
            "|-- RGUID: string (nullable = true)\n " +
            "|-- Query: string (nullable = true)\n " +
            "|-- imprs: integer (nullable = true)\n " +
            "|-- clicks: integer (nullable = true)"))
    }
  }

  test("Verify rootPaths for given LogicalRelations") {
    withTempPath { p =>
      val path1 = new Path(p.getCanonicalPath, "t1").toString
      val path2 = new Path(p.getCanonicalPath, "t2").toString

      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(2)
        .write
        .parquet(path1)

      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(path2)

      // For Spark 3.1 - pathOptionBehavior must be enabled manually
      // for inconsistent use of option("path") and load(paths...)
      withSQLConf("spark.sql.legacy.pathOptionBehavior.enabled" -> "true") {
        Seq(
          (spark.read.format("parquet").load(path1), Seq(path1), 2),
          (spark.read.format("parquet").load(path1, path2), Seq(path1, path2), 5),
          (spark.read.parquet(path1), Seq(path1), 2),
          (spark.read.parquet(path1, path2), Seq(path1, path2), 5),
          (spark.read.parquet(path1, path1, path1), Seq(path1, path1, path1), 6),
          (spark.read.format("parquet").option("path", path1).load(path1), Seq(path1), 2),
          (spark.read.format("parquet").option("path", path1).load(path2), Seq(path2), 3),
          (spark.read.option("path", path1).parquet(path1), Seq(path1, path1), 4),
          (spark.read.option("path", path1).parquet(path2), Seq(path1, path2), 5),
          (
            spark.read.format("parquet").option("path", path1).load(path1, path2),
            Seq(path1, path1, path2),
            7),
          (spark.read.option("path", path1).parquet(path1, path2), Seq(path1, path1, path2), 7))
          .foreach {
            case (df, expectedPaths, expectedCount) =>
              val relation = CreateActionBaseWrapper.getSourceRelations(df).head
              def normalize(path: String): String = {
                new Path(path).toUri.getPath
              }
              assert(relation.rootPaths.map(normalize) == expectedPaths.map(normalize))
              assert(df.count == expectedCount)
              assert(!relation.options.isDefinedAt("path"))
          }
      }
    }
  }
}
