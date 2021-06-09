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
import org.apache.iceberg.{PartitionSpec => IcebergPartitionSpec, Table, TableProperties}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import scala.collection.JavaConverters._

import com.microsoft.hyperspace.{Hyperspace, TestConfig}

class HybridScanForIcebergTest extends HybridScanSuite {
  override protected val fileFormat = "iceberg"
  override protected val fileFormat2 = "iceberg"

  override def beforeAll() {
    super.beforeAll()
    spark.conf.set(
      "spark.hyperspace.index.sources.fileBasedBuilders",
      "com.microsoft.hyperspace.index.sources.iceberg.IcebergFileBasedSourceBuilder," +
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
    val dfFromSampleRenamed = dfFromSample.withColumnRenamed("Date", "date")
    val icebergTable = createIcebergTable(sourcePath, dfFromSampleRenamed, Seq("RGUID"))
    dfFromSampleRenamed.write.format(sourceFileFormat).mode("overwrite").save(sourcePath)
    val df = spark.read.format(sourceFileFormat).load(sourcePath)
    hyperspace.createIndex(df, indexConfig)
    icebergTable.refresh()
    val inputFiles = icebergTable
      .currentSnapshot()
      .addedFiles()
      .asScala
      .toSeq
      .map(f => s"${f.path().toString}")

    // Create a new version by deleting entries.
    if (deleteCnt > 0) {
      val filesToDelete = inputFiles.dropRight(inputFiles.size - deleteCnt)
      val deleteAction = icebergTable.newDelete()
      filesToDelete.foreach(deleteAction.deleteFile)
      deleteAction.commit()
    }
    if (appendCnt > 0) {
      dfFromSampleRenamed
        .limit(appendCnt)
        .write
        .format(sourceFileFormat)
        .mode("append")
        .save(sourcePath)
    }

    icebergTable.refresh()

    val inputFilesA = inputFiles.map(f => s"file:/${f.replaceAll("^/", "")}")
    val inputFilesB = icebergTable
      .newScan()
      .planFiles()
      .iterator()
      .asScala
      .toSeq
      .map(f => s"file:/${f.file().path().toString.replaceAll("^/", "")}")
    (inputFilesB diff inputFilesA, inputFilesA diff inputFilesB)
  }

  test(
    "Append-only: filter rule & parquet format, " +
      "index relation should include appended file paths.") {
    // This test is for verifying plan transformation if appended files could be load with index
    // data scan node. Currently, it's applied for a very specific case: FilterIndexRule,
    // Parquet source format, no partitioning, no deleted files.

    withTempPathAsString { testPath =>
      {
        // Create data & index without helper function as non-partitioned data is required.
        createIcebergTable(testPath, dfFromSample)
        dfFromSample.write.format("iceberg").mode("overwrite").save(testPath)
        val df = spark.read.format("iceberg").load(testPath)
        hyperspace.createIndex(df, indexConfig1.copy(indexName = "index_Append"))

        dfFromSample
          .limit(2)
          .write
          .format("iceberg")
          .mode("append")
          .save(testPath)
      }

      val df = spark.read.format("iceberg").load(testPath)
      def filterQuery: DataFrame =
        df.filter(df("clicks") <= 2000).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      val sourcePath = new Path(testPath).toString

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
        // Filter Index and Parquet format source file can be handled with 1 LogicalRelation.
        assert(nodes.length === 1)
        checkAnswer(baseQuery, filter)
      }
    }
  }

  def createIcebergTable(
      dataPath: String,
      sourceDf: DataFrame,
      partitionCols: Seq[String] = Seq.empty): Table = {
    val props = Map(TableProperties.WRITE_NEW_DATA_LOCATION -> dataPath).asJava
    val schema = SparkSchemaUtil.convert(sourceDf.schema)
    val spec = if (partitionCols.nonEmpty) {
      val s = IcebergPartitionSpec.builderFor(schema)
      partitionCols.foreach(s.identity)
      s.build()
    } else {
      IcebergPartitionSpec.unpartitioned()
    }
    new HadoopTables().create(schema, spec, props, dataPath)
  }
}
