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

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.{Hyperspace, TestConfig}

class HybridScanForDeltaLakeTest extends HybridScanSuite {
  override protected val fileFormat = "delta"
  override protected val fileFormat2 = "delta"

  override def beforeAll() {
    super.beforeAll()
    spark.conf.set(
      "spark.hyperspace.index.sources.fileBasedBuilders",
      "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder," +
        "com.microsoft.hyperspace.index.sources.default.DefaultFileBasedSourceBuilder")
    hyperspace = new Hyperspace(spark)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.conf.unset("spark.hyperspace.index.sources.fileBasedBuilders")
  }

  override def setupIndexAndChangeData(
      sourceFileFormat: String,
      sourcePath: String,
      indexConfig: IndexConfigTrait,
      appendCnt: Int,
      deleteCnt: Int): (Seq[String], Seq[String]) = {
    // Use partition to control the number of appended and deleted files.
    dfFromSample.write.partitionBy("RGUID").format(sourceFileFormat).save(sourcePath)
    val df = spark.read.format(sourceFileFormat).load(sourcePath)
    hyperspace.createIndex(df, indexConfig)
    val inputFiles = df.inputFiles

    // Create a new version by deleting entries.
    val deltaTable = DeltaTable.forPath(sourcePath)
    if (deleteCnt > 0) {
      val sourceDF = spark.read
        .option("basePath", sourcePath)
        .parquet(df.inputFiles.toSeq.dropRight(df.inputFiles.length - deleteCnt): _*)
      val ids = sourceDF.select("RGUID").collect.map(_.getString(0)).toSeq
      var condStr = "RGUID = \"" + ids.head + "\""
      ids.tail.foreach {
        condStr += " OR RGUID = \"" + _ + "\""
      }
      deltaTable.delete(condStr)
    }
    if (appendCnt > 0) {
      dfFromSample
        .limit(appendCnt)
        .write
        .format(sourceFileFormat)
        .mode("append")
        .save(sourcePath)
    }

    val df2 = spark.read.format(sourceFileFormat).load(sourcePath)
    (df2.inputFiles diff inputFiles, inputFiles diff df2.inputFiles)
  }

  test(
    "Append-only: filter rule & parquet format, " +
      "index relation should include appended file paths.") {
    // This flag is for testing plan transformation if appended files could be load with index
    // data scan node. Currently, it's applied for a very specific case: FilterIndexRule,
    // Parquet source format, no partitioning, no deleted files.
    withTempPathAsString { testPath =>
      {
        // Create data & index without helper function as non-partitioned data is required.
        dfFromSample.write.format("delta").save(testPath)
        val df = spark.read.format("delta").load(testPath)
        hyperspace.createIndex(df, indexConfig1.copy(indexName = "index_Append"))

        dfFromSample
          .limit(2)
          .write
          .format("delta")
          .mode("append")
          .save(testPath)
      }

      val df = spark.read.format("parquet").load(testPath)
      def filterQuery: DataFrame =
        df.filter(df("clicks") <= 2000).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      val sourcePath = (new Path(testPath)).toString

      withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        // Check appended file is added to relation node or not.
        val nodes = planWithHybridScan.collect {
          case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            // Verify appended file is included or not.
            val files = fsRelation.location.inputFiles
            assert(files.count(_.contains(sourcePath)) === 2)
            // Verify number of index data files.
            assert(files.count(_.contains("index_Append")) === 4)
            assert(files.length === 6)
            p
        }
        // Filter Index and Parquet format source file can be handled with 1 LogicalRelation
        assert(nodes.length === 1)
        checkAnswer(baseQuery, filter)
      }
    }
  }
}
