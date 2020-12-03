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
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.mockito.Mockito._

import com.microsoft.hyperspace.{HyperspaceException, SampleData, SparkInvolvedSuite}
import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.FileUtils

class CreateActionTest extends SparkFunSuite with SparkInvolvedSuite with SQLHelper {
  private val indexSystemPath = "src/test/resources/indexLocation"
  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet"
  private val indexConfig = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private var df: DataFrame = _

  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])

  object CreateActionBaseWrapper extends CreateActionBase(mockDataManager) {
    def getSourceRelations(df: DataFrame): Seq[Relation] = sourceRelations(spark, df)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, indexSystemPath)
    when(mockLogManager.getLatestLog()).thenReturn(None)
    when(mockLogManager.getLatestId()).thenReturn(None)

    import spark.implicits._
    FileUtils.delete(new Path(indexSystemPath))
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
      ex.getMessage.contains("Only creating index over HDFS file based scan nodes is supported"))
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

  test("op() fails if index config is of wrong case and spark is case-sensitive") {
    when(mockLogManager.getLatestLog()).thenReturn(Some(TestLogEntry(ACTIVE)))
    val indexConfig = IndexConfig("index1", Seq("rgUID"), Seq("dATE"))
    val action = new CreateAction(spark, df, indexConfig, mockLogManager, mockDataManager)
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      val ex = intercept[HyperspaceException](action.op())
      assert(
        ex.getMessage.contains("Columns 'rgUID,dATE' could not be resolved from available " +
          "source columns 'Date,RGUID,Query,imprs,clicks'"))
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
