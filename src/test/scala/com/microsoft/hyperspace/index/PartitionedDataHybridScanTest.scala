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
import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.{Hyperspace, Implicits}
import com.microsoft.hyperspace.util.FileUtils

class PartitionedDataHybridScanTest extends HybridScanTest with HyperspaceSuite {

  // Test cases in HybridScanTest will be executed with indexes on partitioned data sources.
  override def beforeAll(): Unit = {
    super[HyperspaceSuite].beforeAll()
    import spark.implicits._
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    hyperspace = new Hyperspace(spark)

    FileUtils.delete(new Path(sampleDataLocationRoot))
    dfFromSample.write
      .partitionBy("clicks", "Date")
      .format(fileFormat)
      .save(sampleDataFormatAppend)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatAppend2)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatDelete)
    dfFromSample.write.partitionBy("imprs").format(fileFormat).save(sampleDataFormatDelete2)
    dfFromSample.write
      .partitionBy("Date", "imprs")
      .format(fileFormat)
      .save(sampleDataFormatDelete3)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatDelete4)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatBoth)
    dfFromSample.write
      .partitionBy("Date", "clicks")
      .format(fileFormat2)
      .save(sampleDataFormat2Append)
    dfFromSample.write.partitionBy("Date").format(fileFormat2).save(sampleDataFormat2Delete)

    val indexConfig1 = IndexConfig("indexType1", Seq("clicks"), Seq("query"))
    val indexConfig2 = IndexConfig("indexType2", Seq("clicks"), Seq("Date"))

    setupIndexAndChangeData(
      fileFormat,
      sampleDataFormatAppend,
      indexConfig1.copy(indexName = "index_ParquetAppend"),
      appendCnt = 1,
      deleteCnt = 0)
    setupIndexAndChangeData(
      fileFormat,
      sampleDataFormatAppend2,
      indexConfig2.copy(indexName = "indexType2_ParquetAppend2"),
      appendCnt = 1,
      deleteCnt = 0)
    setupIndexAndChangeData(
      fileFormat2,
      sampleDataFormat2Append,
      indexConfig1.copy(indexName = "index_JsonAppend"),
      appendCnt = 1,
      deleteCnt = 0)

    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatDelete,
        indexConfig1.copy(indexName = "index_ParquetDelete"),
        appendCnt = 0,
        deleteCnt = 2)
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatDelete3,
        indexConfig2.copy(indexName = "indexType2_ParquetDelete3"),
        appendCnt = 0,
        deleteCnt = 2)
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatBoth,
        indexConfig1.copy(indexName = "index_ParquetBoth"),
        appendCnt = 1,
        deleteCnt = 1)
      setupIndexAndChangeData(
        fileFormat2,
        sampleDataFormat2Delete,
        indexConfig1.copy(indexName = "index_JsonDelete"),
        appendCnt = 0,
        deleteCnt = 2)
    }
  }

  test("Verify Hybrid Scan for newly added partition after index creation.") {
    withTempPathAsString { testPath =>
      import spark.implicits._
      val (testData, appendData) = sampleData.partition(_._1 != "2019-10-03")
      assert(testData.nonEmpty)
      assert(appendData.nonEmpty)
      testData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .write
        .partitionBy("Date")
        .format(fileFormat)
        .save(testPath)

      val df = spark.read.format(fileFormat).load(testPath)
      hyperspace.createIndex(df, IndexConfig("indexName", Seq("Date"), Seq("clicks")))
      def filterQuery(df: DataFrame): DataFrame = {
        df.filter(df("Date") > "2000-01-01").select(df("clicks"))
      }
      withIndex("indexName") {
        // Check the index is applied regardless of Hybrid Scan config.
        {
          spark.disableHyperspace()
          val baseQuery = filterQuery(df)
          val basePlan = baseQuery.queryExecution.optimizedPlan
          spark.enableHyperspace()

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }

        // Append data creating new partition.
        appendData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .write
          .partitionBy("Date")
          .mode("append")
          .format(fileFormat)
          .save(testPath)

        {
          // Check the index is applied with newly added partition using Hybrid Scan.
          val df = spark.read.format(fileFormat).load(testPath)
          spark.disableHyperspace()
          val baseQuery = filterQuery(df)
          val basePlan = baseQuery.queryExecution.optimizedPlan
          spark.enableHyperspace()

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            val filter = filterQuery(df)
            assert(basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }
      }
    }
  }

  test("Verify Hybrid Scan for partitioned source data with user specified basePath.") {
    withTempPathAsString { testPath =>
      import spark.implicits._
      val (testData, appendData) = sampleData.partition(_._1 != "2019-10-03")
      assert(testData.nonEmpty)
      assert(appendData.nonEmpty)
      testData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .write
        .partitionBy("Date")
        .format(fileFormat)
        .save(testPath)

      // Create an index only for "Data=2017-09-03" partition.
      val indexSourcePath = testPath + "/Date=2017-09-03"
      val df = spark.read.format(fileFormat).option("basePath", testPath).load(indexSourcePath)
      hyperspace.createIndex(df, IndexConfig("indexName", Seq("Date"), Seq("clicks")))
      def filterQuery(df: DataFrame): DataFrame = {
        df.filter(df("Date") > "2000-01-01").select(df("clicks"))
      }

      withIndex("indexName") {
        // Check the index is applied regardless of Hybrid Scan config.
        {
          spark.disableHyperspace()
          val baseQuery = filterQuery(df)
          val basePlan = baseQuery.queryExecution.optimizedPlan
          spark.enableHyperspace()

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }

        // Append data creating new partition.
        appendData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .write
          .partitionBy("Date")
          .mode("append")
          .format(fileFormat)
          .save(testPath)

        {
          // Check the index is applied with newly added partition using Hybrid Scan.
          val df = spark.read.format(fileFormat).option("basePath", testPath).load(testPath)
          spark.disableHyperspace()
          val baseQuery = filterQuery(df)
          val basePlan = baseQuery.queryExecution.optimizedPlan
          spark.enableHyperspace()

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            val filter = filterQuery(df)
            assert(basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }
      }
    }
  }

}
