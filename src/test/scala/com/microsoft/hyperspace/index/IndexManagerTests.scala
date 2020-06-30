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
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, SampleData, SparkInvolvedSuite}
import com.microsoft.hyperspace.TestUtils.copyWithState
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.serde.LogicalPlanSerDeUtils
import com.microsoft.hyperspace.util.FileUtils

class IndexManagerTests extends SparkFunSuite with SparkInvolvedSuite {
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet"
  private val indexStorageLocation = "src/test/resources/indexLocation"
  private val indexConfig1 = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private val indexConfig2 = IndexConfig("index2", Seq("Query"), Seq("imprs"))
  private lazy val hyperspace: Hyperspace = new Hyperspace(spark)
  private var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, indexStorageLocation)

    FileUtils.delete(new Path(indexStorageLocation))
    FileUtils.delete(new Path(sampleParquetDataLocation))

    import spark.implicits._
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .write
      .parquet(sampleParquetDataLocation)

    df = spark.read.parquet(sampleParquetDataLocation)
  }

  after {
    FileUtils.delete(new Path(indexStorageLocation))
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  test("Verify that indexes() returns the correct dataframe.") {
    import spark.implicits._
    hyperspace.createIndex(df, indexConfig1)
    val actual = hyperspace.indexes.as[IndexSummary].collect()(0)
    val expected = new IndexSummary(
      indexConfig1.indexName,
      indexConfig1.indexedColumns,
      indexConfig1.includedColumns,
      200,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))).json,
      s"$indexStorageLocation/index1/v__=0",
      df.queryExecution.optimizedPlan.toString(),
      Constants.States.ACTIVE)
    assert(actual.equals(expected))
  }

  test("Verify getIndexes()") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify if indexes returned match the actual created indexes.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))
  }

  test("Verify delete().") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify expected list of indexes before delete.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))

    hyperspace.deleteIndex("index1")
    hyperspace.deleteIndex("INDEX2")

    // Verify if delete index works and marks the index as `DELETED` in given list of indexes.
    val expectedDeletedIndex1 = copyWithState(expectedIndex1, Constants.States.DELETED)
    val expectedDeletedIndex2 = copyWithState(expectedIndex2, Constants.States.DELETED)
    verifyIndexes(Seq(expectedDeletedIndex1, expectedDeletedIndex2))
  }

  test("Verify restore().") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify expected list of indexes before delete.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))

    hyperspace.deleteIndex("index1")
    hyperspace.deleteIndex("INDEX2")

    // Verify if delete index works before restore.
    val expectedDeletedIndex1 = copyWithState(expectedIndex1, Constants.States.DELETED)
    val expectedDeletedIndex2 = copyWithState(expectedIndex2, Constants.States.DELETED)
    verifyIndexes(Seq(expectedDeletedIndex1, expectedDeletedIndex2))

    // Check if index1 got restored and `DELETED` flag is `CREATED` in the list of indexes.
    hyperspace.restoreIndex("index1")
    hyperspace.restoreIndex("INDEX2")
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))
  }

  test("Verify vacuum().") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify expected list of indexes before delete.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))

    hyperspace.deleteIndex("index1")
    hyperspace.deleteIndex("INDEX2")

    // Verify if delete index works and marks the index as `DELETED` in given list of indexes.
    val expectedDeletedIndex1 = copyWithState(expectedIndex1, Constants.States.DELETED)
    val expectedDeletedIndex2 = copyWithState(expectedIndex2, Constants.States.DELETED)

    verifyIndexes(Seq(expectedDeletedIndex1, expectedDeletedIndex2))

    // Verify if after vacuum `DELETED` index are not part of the list of indexes.
    hyperspace.vacuumIndex("index1")
    hyperspace.vacuumIndex("INDEX2")
    val expectedVacuumedIndex1 = copyWithState(expectedIndex1, Constants.States.DOESNOTEXIST)
    val expectedVacuumedIndex2 = copyWithState(expectedIndex2, Constants.States.DOESNOTEXIST)
    verifyIndexes(Seq(expectedVacuumedIndex1, expectedVacuumedIndex2))
  }

  test("Verify refresh-full rebuild of index.") {
    val refreshTestLocation = sampleParquetDataLocation + "refresh"
    FileUtils.delete(new Path(refreshTestLocation))
    import spark.implicits._
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .limit(10)
      .write
      .parquet(refreshTestLocation)
    val df = spark.read.parquet(refreshTestLocation)
    hyperspace.createIndex(df, indexConfig1)
    var indexCount =
      spark.read
        .parquet(s"$indexStorageLocation/index1" +
          s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
        .count()
    assert(indexCount == 10)

    // Change Original Data
    SampleData.testData
      .toDF("Date", "RGUID", "Query", "imprs", "clicks")
      .limit(3)
      .write
      .mode("overwrite")
      .parquet(refreshTestLocation)
    hyperspace.refreshIndex(indexConfig1.indexName)
    indexCount = spark.read
      .parquet(s"$indexStorageLocation/index1" +
        s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")
      .count()

    // Check if index got updated
    assert(indexCount == 3)

    FileUtils.delete(new Path(refreshTestLocation))
  }

  private def expectedIndex(
      indexConfig: IndexConfig,
      schema: StructType,
      df: DataFrame,
      state: String = Constants.States.ACTIVE): IndexLogEntry = {

    LogicalPlanSignatureProvider.create().signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val serializedPlan = LogicalPlanSerDeUtils.serialize(df.queryExecution.logical, spark)
        val sourcePlanProperties = SparkPlan.Properties(
          serializedPlan,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(
              Seq(Signature(
                LogicalPlanSignatureProvider.create().name,
                s)))))
        val sourceFiles = df.queryExecution.optimizedPlan.collect {
          case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
            location.allFiles.map(_.getPath.toString)
        }.flatten
        val sourceDataProperties =
          Hdfs.Properties(Content("", Seq(Content.Directory("", sourceFiles, NoOpFingerprint()))))

        val entry = IndexLogEntry(
          indexConfig.indexName,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(indexConfig.indexedColumns, indexConfig.includedColumns),
              IndexLogEntry.schemaString(schema),
              IndexConstants.INDEX_NUM_BUCKETS_DEFAULT)),
          Content(
            s"$indexStorageLocation/${indexConfig.indexName}" +
              s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0",
            Seq()),
          Source(SparkPlan(sourcePlanProperties), Seq(Hdfs(sourceDataProperties))),
          Map())
        entry.state = state
        entry
      case None => throw HyperspaceException("Invalid plan for index dataFrame.")
    }
  }

  // Verify if the indexes currently stored in Hyperspace matches the given indexes.
  private def verifyIndexes(expectedIndexes: Seq[IndexLogEntry]): Unit = {
    assert(IndexCollectionManager(spark).getIndexes().toSet == expectedIndexes.toSet)
  }
}
