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
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.{Hyperspace, TestConfig}
import com.microsoft.hyperspace.TestUtils.latestIndexLogEntry
import com.microsoft.hyperspace.util.FileUtils

// Hybrid Scan tests for non partitioned source data. Test cases of HybridScanSuite are also
// executed with non partitioned source data.
class HybridScanForNonPartitionedDataTest extends HybridScanSuite {
  override val indexLocationDirName = "hybridScanTest"

  override def beforeAll(): Unit = {
    super.beforeAll()
    hyperspace = new Hyperspace(spark)
  }

  test(
    "Append-only: filter rule & parquet format, " +
      "index relation should include appended file paths.") {
    // This flag is for testing plan transformation if appended files could be load with index
    // data scan node. Currently, it's applied for a very specific case: FilterIndexRule,
    // Parquet source format, no partitioning, no deleted files.
    withTempPathAsString { testPath =>
      val (appendedFiles, deletedFiles) = setupIndexAndChangeData(
        "parquet",
        testPath,
        indexConfig1.copy(indexName = "index_Append"),
        appendCnt = 1,
        deleteCnt = 0)

      val df = spark.read.format("parquet").load(testPath)
      def filterQuery: DataFrame =
        df.filter(df("clicks") <= 2000).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        // Check appended file is added to relation node or not.
        val nodes = planWithHybridScan.collect {
          case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            // Verify appended file is included or not.
            val files = fsRelation.location.inputFiles
            assert(files.count(_.equals(appendedFiles.head)) === 1)
            // Verify number of index data files.
            assert(files.count(_.contains("index_Append")) === 4)
            assert(files.length === 5)
            p
        }
        // Filter Index and Parquet format source file can be handled with 1 LogicalRelation
        assert(nodes.length === 1)
        checkAnswer(baseQuery, filter)
      }
    }
  }

  test("Delete-only: Hybrid Scan for delete support doesn't work without lineage column.") {
    val indexConfig = IndexConfig("index_ParquetDelete2", Seq("clicks"), Seq("query"))
    Seq(("indexWithoutLineage", "false", false), ("indexWithLineage", "true", true)) foreach {
      case (indexName, lineageColumnConfig, transformationExpected) =>
        withTempPathAsString { testPath =>
          withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> lineageColumnConfig) {
            setupIndexAndChangeData(
              fileFormat,
              testPath,
              indexConfig.copy(indexName = indexName),
              appendCnt = 0,
              deleteCnt = 1)

            val df = spark.read.format(fileFormat).load(testPath)

            def filterQuery: DataFrame =
              df.filter(df("clicks") <= 2000).select(df("query"))

            val baseQuery = filterQuery
            val basePlan = baseQuery.queryExecution.optimizedPlan
            withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
              val filter = filterQuery
              assert(basePlan.equals(filter.queryExecution.optimizedPlan))
            }
            withSQLConf(TestConfig.HybridScanEnabled: _*) {
              val filter = filterQuery
              assert(
                basePlan
                  .equals(filter.queryExecution.optimizedPlan)
                  .equals(!transformationExpected))
            }
          }
        }
    }
  }

  test("Delete-only: filter rule, number of delete files threshold.") {
    withTempPathAsString { testPath =>
      val indexName = "IndexDeleteCntTest"
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        setupIndexAndChangeData(
          fileFormat,
          testPath,
          indexConfig1.copy(indexName = indexName),
          appendCnt = 0,
          deleteCnt = 2)
      }

      val df = spark.read.format(fileFormat).load(testPath)
      def filterQuery: DataFrame =
        df.filter(df("clicks") <= 2000).select(df("query"))
      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan
      val sourceSize = latestIndexLogEntry(systemPath, indexName).sourceFilesSizeInBytes

      val afterDeleteSize = FileUtils.getDirectorySize(new Path(testPath))
      val deletedRatio = 1 - (afterDeleteSize / sourceSize.toFloat)

      withSQLConf(TestConfig.HybridScanEnabled: _*) {
        withSQLConf(
          IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD ->
            (deletedRatio + 0.1).toString) {
          val filter = filterQuery
          // As deletedRatio is less than the threshold, the index can be applied.
          assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
        }
        withSQLConf(
          IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD ->
            (deletedRatio - 0.1).toString) {
          val filter = filterQuery
          // As deletedRatio is greater than the threshold, the index shouldn't be applied.
          assert(basePlan.equals(filter.queryExecution.optimizedPlan))
        }
      }
    }
  }
}
