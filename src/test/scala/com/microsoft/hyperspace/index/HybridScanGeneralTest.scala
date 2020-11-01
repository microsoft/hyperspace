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

import com.microsoft.hyperspace.Hyperspace

class HybridScanGeneralTest extends HybridScanTestSuite {
  override val systemPath = new Path("src/test/resources/hybridScanTest")

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    hyperspace = new Hyperspace(spark)

    dfFromSample.write.format(fileFormat).save(sampleDataFormatAppend)
    dfFromSample.write.format(fileFormat).save(sampleDataFormatAppend2)
    dfFromSample.write.format(fileFormat).save(sampleDataFormatDelete)
    dfFromSample.write.format(fileFormat).save(sampleDataFormatDelete2)
    dfFromSample.write.format(fileFormat).save(sampleDataFormatDelete3)
    dfFromSample.write.format(fileFormat).save(sampleDataFormatDelete4)
    dfFromSample.write.format(fileFormat).save(sampleDataFormatBoth)
    dfFromSample.write.format(fileFormat2).save(sampleDataFormat2Append)
    dfFromSample.write.format(fileFormat2).save(sampleDataFormat2Delete)

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

  test(
    "Append-only: filter rule & parquet format, " +
      "index relation should include appended file paths") {
    // This flag is for testing plan transformation if appended files could be load with index
    // data scan node. Currently, it's applied for a very specific case: FilterIndexRule,
    // Parquet source format, no partitioning, no deleted files.
    val df = spark.read.format(fileFormat).load(sampleDataFormatAppend)
    def filterQuery: DataFrame =
      df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
      val filter = filterQuery
      assert(basePlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
      val filter = filterQuery
      val planWithHybridScan = filter.queryExecution.optimizedPlan
      assert(!basePlan.equals(planWithHybridScan))

      // Check appended file is added to relation node or not.
      val nodes = planWithHybridScan.collect {
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          // Verify appended file is included or not.
          assert(fsRelation.location.inputFiles.count(_.contains(sampleDataFormatAppend)) === 1)
          // Verify number of index data files.
          assert(fsRelation.location.inputFiles.count(_.contains("index_ParquetAppend")) === 4)
          assert(fsRelation.location.inputFiles.length === 5)
          p
      }
      // Filter Index and Parquet format source file can be handled with 1 LogicalRelation
      assert(nodes.length === 1)
      checkAnswer(baseQuery, filter)
    }
  }

  test("Delete-only: Hybrid Scan for delete support doesn't work without lineage column") {
    val indexConfig = IndexConfig("index_ParquetDelete2", Seq("clicks"), Seq("query"))
    Seq(("indexWithoutLineage", "false", false), ("indexWithLineage", "true", true)) foreach {
      case (indexName, lineageColumnConfig, transformationExpected) =>
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> lineageColumnConfig) {
          setupIndexAndChangeData(
            fileFormat,
            sampleDataFormatDelete2,
            indexConfig.copy(indexName = indexName),
            appendCnt = 0,
            deleteCnt = 1)

          val df = spark.read.format(fileFormat).load(sampleDataFormatDelete2)
          def filterQuery: DataFrame =
            df.filter(df("clicks") <= 2000).select(df("query"))
          val baseQuery = filterQuery
          val basePlan = baseQuery.queryExecution.optimizedPlan
          withSQLConf(
            IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
            IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "false") {
            val filter = filterQuery
            assert(basePlan.equals(filter.queryExecution.optimizedPlan))
          }
          withSQLConf(
            IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
            IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true") {
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
