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

import scala.collection.mutable.WrappedArray

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.functions._

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, SampleNestedData}
import com.microsoft.hyperspace.util.{FileUtils, SchemaUtils}

class CreateIndexNestedTest extends HyperspaceSuite with SQLHelper {
  override val systemPath = new Path("src/test/resources/indexLocation")
  private val testDir = "src/test/resources/createIndexTests/"
  private val nonPartitionedDataPath = testDir + "samplenestedparquet"
  private val partitionedDataPath = testDir + "samplenestedpartitionedparquet"
  private val partitionKeys = Seq("Date", "Query")
  private val indexConfig1 =
    IndexConfig("index1", Seq("nested.leaf.id"), Seq("Date", "nested.leaf.cnt"))
  private val indexConfig2 = IndexConfig("index3", Seq("nested.leaf.id"), Seq("nested.leaf.cnt"))
  private var nonPartitionedDataDF: DataFrame = _
  private var partitionedDataDF: DataFrame = _
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(testDir), isRecursive = true)

    val dataColumns = Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested")
    // save test data non-partitioned.
    SampleNestedData.save(spark, nonPartitionedDataPath, dataColumns)
    nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    // save test data partitioned.
    SampleNestedData.save(spark, partitionedDataPath, dataColumns, Some(partitionKeys))
    partitionedDataDF = spark.read.parquet(partitionedDataPath)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(testDir), isRecursive = true)
    super.afterAll()
  }

  after {
    FileUtils.delete(systemPath)
  }

  test("Index creation with nested indexed and included columns") {
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
    assert(hyperspace.indexes.where(s"name = 'index1' ").count == 1)
    assert(hyperspace.indexes.where(
      array_contains(col("indexedColumns"), "nested__leaf__id")).count == 1)
    assert(hyperspace.indexes.where(
      array_contains(col("includedColumns"), "nested__leaf__cnt")).count == 1)
    val colTypes = hyperspace.indexes.select("schema")
      .collect().map(r => r.getString(0)).head
    assert(colTypes.contains("nested__leaf__id"))
    assert(colTypes.contains("nested__leaf__cnt"))
  }

  test("Index creation passes with columns of different case if case-sensitivity is false.") {
    hyperspace.createIndex(
      nonPartitionedDataDF,
      IndexConfig("index1", Seq("Nested.leaF.id"), Seq("nested.leaf.CNT")))
    val indexes = hyperspace.indexes.where(s"name = 'index1' ")
    assert(indexes.count == 1)
    assert(
      indexes.head.getAs[WrappedArray[String]]("indexedColumns").head == "nested__leaf__id",
      "Indexed columns with wrong case are stored in metadata")
    assert(
      indexes.head.getAs[WrappedArray[String]]("includedColumns").head == "nested__leaf__cnt",
      "Included columns with wrong case are stored in metadata")
  }

  test("Index creation fails with columns of different case if case-sensitivity is true.") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      val exception = intercept[HyperspaceException] {
        hyperspace.createIndex(
          nonPartitionedDataDF,
          IndexConfig("index1", Seq("Nested.leaF.id"), Seq("nested.leaf.CNT")))
      }
      assert(exception.getMessage.contains("Index config is not applicable to dataframe schema."))
    }
  }

  test("Index creation fails since the dataframe has a filter node.") {
    val dfWithFilter = nonPartitionedDataDF.filter("nested.leaf.id='leaf_id1'")
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfWithFilter, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test("Index creation fails since the dataframe has a projection node.") {
    val dfWithSelect = nonPartitionedDataDF.select("nested.leaf.id")
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfWithSelect, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test("Index creation fails since the dataframe has a join node.") {
    val dfJoin = nonPartitionedDataDF
      .join(nonPartitionedDataDF, nonPartitionedDataDF("Query") === nonPartitionedDataDF("Query"))
      .select(
        nonPartitionedDataDF("RGUID"),
        nonPartitionedDataDF("Query"),
        nonPartitionedDataDF("nested.leaf.cnt"))
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfJoin, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test("Check lineage in index records for partitioned data when partition key is not in config.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(partitionedDataDF, indexConfig2)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig2.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For partitioned data, beside file name lineage column all partition keys columns
      // should be added to index schema if they are not already among index config columns.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          (SchemaUtils.escapeFieldNames(indexConfig2.indexedColumns ++
            indexConfig2.includedColumns) ++
            Seq(IndexConstants.DATA_FILE_NAME_ID) ++ partitionKeys).sorted)
    }
  }

  test("Check lineage in index records for non-partitioned data.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig1.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For non-partitioned data, only file name lineage column should be added to index schema.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          (SchemaUtils.escapeFieldNames(indexConfig1.indexedColumns ++
            indexConfig1.includedColumns) ++
            Seq(IndexConstants.DATA_FILE_NAME_ID)).sorted)
    }
  }

  test("Verify content of lineage column.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      val dataPath = new Path(nonPartitionedDataPath, "*parquet")
      val dataFilesCount = dataPath
        .getFileSystem(new Configuration)
        .globStatus(dataPath)
        .length
        .toLong

      // File ids are assigned incrementally starting from 0.
      val lineageRange = 0L to dataFilesCount

      hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig1.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
      val lineageValues = indexRecordsDF
        .select(IndexConstants.DATA_FILE_NAME_ID)
        .distinct()
        .collect()
        .map(r => r.getLong(0))

      lineageValues.forall(lineageRange.contains(_))
    }
  }
}
