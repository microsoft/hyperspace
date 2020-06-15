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
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.FileUtils
import com.microsoft.hyperspace.{HyperspaceException, SampleData, SparkInvolvedSuite}

@RunWith(classOf[JUnitRunner])
class CreateActionTest extends FunSuite with SparkInvolvedSuite {
  private val indexSystemPath = "src/test/resources/indexLocation"
  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet"
  private val indexConfig = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private var df: DataFrame = _

  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])

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
    // No exception thrown is considered a pass
    val ex = intercept[HyperspaceException](action.validate())
    assert(
      ex.getMessage.contains(s"Another Index with name ${indexConfig.indexName} already exists"))
  }
}
