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
import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.{Hyperspace, Implicits, TestConfig}
import com.microsoft.hyperspace.util.FileUtils

// Hybrid Scan tests for partitioned source data. Test cases of HybridScanSuite are also
// executed with partitioned source data.
class HybridScanForPartitionedDataTest extends HybridScanSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    hyperspace = new Hyperspace(spark)
  }

  override def setupIndexAndChangeData(
      sourceFileFormat: String,
      sourcePath: String,
      indexConfig: IndexConfigTrait,
      appendCnt: Int,
      deleteCnt: Int): (Seq[String], Seq[String]) = {
    dfFromSample.write.partitionBy("clicks", "Date").format(sourceFileFormat).save(sourcePath)
    val df = spark.read.format(sourceFileFormat).load(sourcePath)
    hyperspace.createIndex(df, indexConfig)
    val inputFiles = df.inputFiles
    assert(appendCnt + deleteCnt < inputFiles.length)

    val fs = systemPath.getFileSystem(new Configuration)
    for (i <- 0 until appendCnt) {
      val sourcePath = new Path(inputFiles(i))
      val destPath = new Path(inputFiles(i) + ".copy")
      fs.copyToLocalFile(sourcePath, destPath)
    }

    for (i <- 1 to deleteCnt) {
      fs.delete(new Path(inputFiles(inputFiles.length - i)), false)
    }

    val df2 = spark.read.format(sourceFileFormat).load(sourcePath)
    (df2.inputFiles diff inputFiles, inputFiles diff df2.inputFiles)
  }

  test("Verify Hybrid Scan for newly added partition after index creation.") {
    withTempPathAsString { testPath =>
      import spark.implicits._
      val (testData, appendData) = sampleData.partition(_._1 != "2019-10-03")
      assert(testData.nonEmpty)
      assert(appendData.nonEmpty)
      testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
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

          withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }

        // Append data creating new partition.
        appendData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
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

          withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
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
      // sampleData has 3 partitions: 2017-09-03, 2018-09-03, 2019-10-03
      val (testData, appendData) = sampleData.partition(_._1 != "2019-10-03")
      assert(testData.nonEmpty)
      assert(appendData.nonEmpty)
      testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .write
        .partitionBy("Date")
        .format(fileFormat)
        .save(testPath)

      // Create an index only for "Date=2017-09-03" and "Date=2018-09-03" partition using user
      // specified basePath. In this way, rootPaths of the index will be both partition paths,
      // not the basePath.
      val indexSourcePaths = Seq(testPath + "/Date=2017-09-03", testPath + "/Date=2018-09-03")
      val df =
        spark.read.format(fileFormat).option("basePath", testPath).load(indexSourcePaths: _*)
      hyperspace.createIndex(df, IndexConfig("indexName", Seq("Date"), Seq("clicks")))
      def filterQuery(df: DataFrame): DataFrame = {
        df.filter(df("Date") > "2000-01-01").select(df("clicks"))
      }

      withIndex("indexName") {
        {
          // Check the index is applied regardless of Hybrid Scan config.
          val df = spark.read.format(fileFormat).load(testPath)
          spark.disableHyperspace()
          val baseQuery = filterQuery(df)
          val basePlan = baseQuery.queryExecution.optimizedPlan
          spark.enableHyperspace()

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }

          withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
            val filter = filterQuery(df)
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }

        // Append data creating new partition, Date=2019-10-03.
        appendData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .write
          .partitionBy("Date")
          .mode("append")
          .format(fileFormat)
          .save(testPath)

        assert(FileUtils.getDirectorySize(new Path(testPath + "/Date=2019-10-03")) > 0)

        {
          // Check the index is applied with newly added partition using Hybrid Scan.
          val df = spark.read.format(fileFormat).load(testPath)
          spark.disableHyperspace()
          val baseQuery = filterQuery(df)
          val basePlan = baseQuery.queryExecution.optimizedPlan
          spark.enableHyperspace()

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            val filter = filterQuery(df)
            // Since rootPaths are partition specific paths - "/Date=2017-09-03" and
            // "/Date=2018-09-03", the index won't be applied with the newly appended partition.
            assert(basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }

          withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
            val filter = filterQuery(df)
            // The new partition can be handled with Hybrid Scan approach.
            assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
            checkAnswer(baseQuery, filter)
          }
        }
      }
    }
  }
}
