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
import com.microsoft.hyperspace.util.FileUtils
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

class CreateIndexNestedTest extends HyperspaceSuite with SQLHelper {
  private val testDir = inTempDir("createIndexTests")
  private val nonPartitionedDataPath = testDir + "/samplenestedparquet"
  private val partitionedDataPath = testDir + "/samplenestedpartitionedparquet"
  private val partitionKeys = Seq("Date", "Query")
  private val indexConfig1 =
    IndexConfig("index1", Seq("nested.leaf.id"), Seq("Date", "nested.leaf.cnt"))
  private val indexConfig2 = IndexConfig("index3", Seq("nested.leaf.id"), Seq("nested.leaf.cnt"))
  private var nonPartitionedDataDF: DataFrame = _
  private var partitionedDataDF: DataFrame = _
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(IndexConstants.DEV_NESTED_COLUMN_ENABLED, "true")
    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(testDir), isRecursive = true)

    val dataColumns = Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested")
    // Save test data non-partitioned.
    SampleNestedData.save(spark, nonPartitionedDataPath, dataColumns)
    nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    // Save test data partitioned.
    SampleNestedData.save(spark, partitionedDataPath, dataColumns, Some(partitionKeys))
    partitionedDataDF = spark.read.parquet(partitionedDataPath)
  }

  override def afterAll(): Unit = {
    spark.conf.unset(IndexConstants.DEV_NESTED_COLUMN_ENABLED)
    FileUtils.delete(new Path(testDir), isRecursive = true)
    super.afterAll()
  }

  after {
    FileUtils.delete(systemPath)
  }

  test("Index creation with nested indexed and included columns.") {
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
    assert(hyperspace.indexes.where(s"name = 'index1' ").count == 1)
    assert(
      hyperspace.indexes
        .where(array_contains(col("indexedColumns"), "__hs_nested.nested.leaf.id"))
        .count == 1)
    assert(
      hyperspace.indexes
        .where(col("additionalStats.includedColumns").contains("__hs_nested.nested.leaf.cnt"))
        .count == 1)
    val colTypes = hyperspace.indexes
      .select("additionalStats.schema")
      .collect()
      .map(r => r.getString(0))
      .head
    assert(colTypes.contains("__hs_nested.nested.leaf.id"))
    assert(colTypes.contains("__hs_nested.nested.leaf.cnt"))
  }

  test("Index creation passes with columns of different case if case-sensitivity is false.") {
    hyperspace.createIndex(
      nonPartitionedDataDF,
      IndexConfig("index1", Seq("Nested.leaF.id"), Seq("nested.leaf.CNT")))
    val indexes = hyperspace.indexes.where(s"name = 'index1' ")
    assert(indexes.count == 1)
    assert(
      indexes.head
        .getAs[WrappedArray[String]]("indexedColumns")
        .head == "__hs_nested.nested.leaf.id",
      "Indexed columns with wrong case are stored in metadata")
    assert(
      indexes.head
        .getAs[Map[String, String]]("additionalStats")(
          "includedColumns") == "__hs_nested.nested.leaf.cnt",
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
    val dfA = nonPartitionedDataDF.as("A")
    val dfB = nonPartitionedDataDF.as("B")
    val dfJoin = dfA
      .join(dfB, dfA("Query") === dfB("Query"))
      .select(dfA("RGUID"), dfA("Query"), dfA("nested.leaf.cnt"))
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfJoin, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test(
    "Check lineage in index records for partitioned data when partition key is not in config.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(partitionedDataDF, indexConfig2)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig2.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For partitioned data, beside file name lineage column all partition keys columns
      // should be added to index schema if they are not already among index config columns.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          ((indexConfig2.indexedColumns ++ indexConfig2.includedColumns).map(
            ResolvedColumn(_, isNested = true).normalizedName) ++
            Seq(IndexConstants.DATA_FILE_NAME_ID) ++ partitionKeys).sorted)
    }
  }

  test("Check lineage in index records for non-partitioned data.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig2)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig2.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For non-partitioned data, only file name lineage column should be added to index schema.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          ((indexConfig2.indexedColumns ++ indexConfig2.includedColumns).map(
            ResolvedColumn(_, isNested = true).normalizedName) ++
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

  test("Disable index creation with nested columns until fully supported.") {
    spark.conf.set(IndexConstants.DEV_NESTED_COLUMN_ENABLED, "false")
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
    }
    assert(exception.getMessage.contains("Hyperspace does not support nested columns yet."))
  }

  test("Verify index creation with StructType column.") {
    val indexConfig = IndexConfig("index1", Seq("nested"), Seq("clicks"))
    val indexConfig2 = IndexConfig("index2", Seq("clicks"), Seq("nested"))
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig)
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig2)
    assert(hyperspace.indexes.where(s"name = 'index1' ").count == 1)
    assert(hyperspace.indexes.where(s"name = 'index2' ").count == 1)

    import com.microsoft.hyperspace._
    spark.enableHyperspace

    {
      val filter = nonPartitionedDataDF.filter(col("nested").isNotNull).select("nested", "clicks")
      assert(
        filter.queryExecution.optimizedPlan.toString
          .contains("Hyperspace(Type: CI, Name: index1"))
    }
    {
      val filter = nonPartitionedDataDF.filter("nested.id = \"id1\"").select("nested", "clicks")
      assert(
        filter.queryExecution.optimizedPlan.toString
          .contains("Hyperspace(Type: CI, Name: index1"))
    }
    {
      val filter =
        nonPartitionedDataDF.filter("nested.id = \"id1\"").select("nested.id", "clicks")
      assert(
        filter.queryExecution.optimizedPlan.toString
          .contains("Hyperspace(Type: CI, Name: index1"))
    }
    {
      val filter = nonPartitionedDataDF.filter("clicks = 1000").select("nested", "clicks")
      assert(
        filter.queryExecution.optimizedPlan.toString
          .contains("Hyperspace(Type: CI, Name: index2"))
    }
  }
}
