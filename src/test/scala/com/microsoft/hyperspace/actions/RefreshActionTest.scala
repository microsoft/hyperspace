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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{mock, when}

import com.microsoft.hyperspace.{HyperspaceException, SampleData, SparkInvolvedSuite}
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, CREATING}
import com.microsoft.hyperspace.index._

class RefreshActionTest extends SparkFunSuite with SparkInvolvedSuite {
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet"
  private val fileSystem = new Path(sampleParquetDataLocation).getFileSystem(new Configuration)
  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])
  private var testLogEntry: LogEntry = _

  object CreateActionBaseWrapper extends CreateActionBase(mockDataManager) {
    def getSourceRelations(df: DataFrame): Seq[Relation] = sourceRelations(spark, df)
  }

  private def updateSourceFiles(): Unit = {
    import spark.implicits._
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .write
      .mode("append")
      .parquet(sampleParquetDataLocation)
  }

  before {
    when(mockLogManager.getLatestId()).thenReturn(None)
    when(mockDataManager.getLatestVersionId()).thenReturn(None)
    when(mockDataManager.getPath(anyInt)).thenReturn(new Path("indexPath"))

    fileSystem.delete(new Path(sampleParquetDataLocation), true)

    import spark.implicits._
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .write
      .parquet(sampleParquetDataLocation)

    val df = spark.read.parquet(sampleParquetDataLocation)
    testLogEntry = testEntry(df)
  }

  def testEntry(df: DataFrame): IndexLogEntry = {
    val sourcePlanProperties = SparkPlan.Properties(
      CreateActionBaseWrapper.getSourceRelations(df),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("signatureProvider", "dfSignature")))))

    val entry = IndexLogEntry(
      "index1",
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(Seq("clicks"), Seq()),
          "schema",
          10,
          Map())),
      Content(Directory("dirPath")),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }

  override def afterAll(): Unit = {
    fileSystem.delete(new Path(sampleParquetDataLocation), true)
    super.afterAll()
  }

  test("validate() passes if old index logs are found with ACTIVE state") {
    testLogEntry.state = ACTIVE
    updateSourceFiles()
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testLogEntry))
    val action = new RefreshAction(spark, mockLogManager, mockDataManager)
    action.validate()
  }

  test("validate() fails if old index logs found with non-ACTIVE state") {
    testLogEntry.state = CREATING
    updateSourceFiles()
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testLogEntry))
    val action = new RefreshAction(spark, mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(ex.getMessage.contains("Refresh is only supported in ACTIVE state"))
  }

  test("validate() fails if there is no source data change.") {
    testLogEntry.state = ACTIVE
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testLogEntry))
    val action = new RefreshAction(spark, mockLogManager, mockDataManager)
    val ex = intercept[NoChangesException](action.validate())
    assert(ex.getMessage.contains("Refresh full aborted as no source data changed."))
  }
}
