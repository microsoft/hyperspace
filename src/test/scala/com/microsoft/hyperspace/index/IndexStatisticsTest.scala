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
import org.apache.spark.sql.{DataFrame, QueryTest}

import com.microsoft.hyperspace.{Hyperspace, SampleData, TestUtils}
import com.microsoft.hyperspace.TestUtils.logManager
import com.microsoft.hyperspace.util.FileUtils

class IndexStatisticsTest extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/indexStatsTest")
  private val dataColumns = Seq("Date", "RGUID", "Query", "imprs", "clicks")
  private val dataPath = "src/test/resources/data/sampleparquet"
  private val indexConfig = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private var dataDF: DataFrame = _
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(dataPath), true)

    SampleData.save(spark, dataPath, dataColumns)
    dataDF = spark.read.parquet(dataPath)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(dataPath), true)
    super.afterAll()
  }

  after {
    FileUtils.delete(systemPath)
  }

  test("getIndexStats() on a fresh index returns correct result.") {
    Seq(true, false).foreach { enableLineage =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
        withIndex(indexConfig.indexName) {
          hyperspace.createIndex(dataDF, indexConfig)
          validateIndexStats(indexConfig.indexName)
        }
      }
    }
  }

  test(
    "getIndexStats() on an index refreshed in incremental or quick mode returns correct result.") {
    Seq("incremental", "quick").foreach { mode =>
      withTempPathAsString { testPath =>
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
          withIndex(indexConfig.indexName) {
            SampleData.save(spark, testPath, Seq("Date", "RGUID", "Query", "imprs", "clicks"))
            val df = spark.read.parquet(testPath)
            hyperspace.createIndex(df, indexConfig)

            // modify source content.
            import spark.implicits._
            TestUtils.deleteFiles(testPath, "*parquet", 1).head
            SampleData.testData
              .take(3)
              .toDF(dataColumns: _*)
              .write
              .mode("append")
              .parquet(testPath)

            hyperspace.refreshIndex(indexConfig.indexName, mode)
            validateIndexStats(indexConfig.indexName)
          }
        }
      }
    }
  }

  test("getIndexStats() on an index with multiple active versions returns correct result.") {
    withTempPathAsString { testPath =>
      withIndex(indexConfig.indexName) {
        SampleData.save(spark, testPath, Seq("Date", "RGUID", "Query", "imprs", "clicks"))
        val df = spark.read.parquet(testPath)
        hyperspace.createIndex(df, indexConfig)

        import spark.implicits._
        for (_ <- 1 to 2) {
          SampleData.testData
            .take(3)
            .toDF(dataColumns: _*)
            .write
            .mode("append")
            .parquet(testPath)

          hyperspace.refreshIndex(indexConfig.indexName, "incremental")
        }
        validateIndexStats(indexConfig.indexName)
      }
    }
  }

  private def validateIndexStats(indexName: String): Unit = {
    val indexStatsDF = hyperspace.getIndexStats(indexName)
    assert(indexStatsDF.count() == 1)

    import spark.implicits._
    val indexStats = indexStatsDF.as[IndexStatistics].collect()(0)
    val log = logManager(systemPath, indexName).getLatestStableLog()
    assert(log.isDefined)
    val entry = log.get.asInstanceOf[IndexLogEntry]
    assert(indexStats.equals(IndexStatistics(spark, entry)))
  }
}
