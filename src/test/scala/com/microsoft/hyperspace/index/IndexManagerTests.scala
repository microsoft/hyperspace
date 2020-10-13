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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, MockEventLogger, SampleData}
import com.microsoft.hyperspace.TestUtils.{copyWithState, logManager}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.IndexConstants.{OPTIMIZE_FILE_SIZE_THRESHOLD, REFRESH_MODE_FULL, REFRESH_MODE_INCREMENTAL}
import com.microsoft.hyperspace.telemetry.OptimizeActionEvent
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

class IndexManagerTests extends HyperspaceSuite with SQLHelper {
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet"
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/indexLocation")
  private val indexConfig1 = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private val indexConfig2 = IndexConfig("index2", Seq("Query"), Seq("imprs"))
  private lazy val hyperspace: Hyperspace = new Hyperspace(spark)
  private var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    FileUtils.delete(new Path(sampleParquetDataLocation))
    SampleData.save(
      spark,
      sampleParquetDataLocation,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    df = spark.read.parquet(sampleParquetDataLocation)
  }

  after {
    FileUtils.delete(systemPath, true)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  test("Verify that indexes() returns the correct dataframe with and without lineage.") {
    Seq(true, false).foreach { enableLineage =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
        withIndex(indexConfig1.indexName) {
          import spark.implicits._
          hyperspace.createIndex(df, indexConfig1)
          val actual = hyperspace.indexes.as[IndexSummary].collect().head
          var expectedSchema =
            StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType)))
          if (enableLineage) {
            expectedSchema =
              expectedSchema.add(StructField(IndexConstants.DATA_FILE_NAME_COLUMN, StringType))
          }
          val expected = new IndexSummary(
            indexConfig1.indexName,
            indexConfig1.indexedColumns,
            indexConfig1.includedColumns,
            200,
            expectedSchema.json,
            s"$systemPath/${indexConfig1.indexName}" +
              s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0",
            Constants.States.ACTIVE)
          assert(actual.equals(expected))
        }
      }
    }
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
    Seq(("csv", Map("header" -> "true")), ("parquet", Map()), ("json", Map())).foreach {
      case (format, option: Map[String, String]) =>
        val refreshTestLocation = sampleParquetDataLocation + "refresh_" + format
        FileUtils.delete(new Path(refreshTestLocation))
        val indexConfig = IndexConfig(s"index_$format", Seq("RGUID"), Seq("imprs"))
        import spark.implicits._
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(10)
          .write
          .options(option)
          .format(format)
          .save(refreshTestLocation)
        val df = spark.read.format(format).options(option).load(refreshTestLocation)
        hyperspace.createIndex(df, indexConfig)
        var indexCount =
          spark.read
            .parquet(s"$systemPath/index_$format" +
              s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
            .count()
        assert(indexCount == 10)

        // Change Original Data
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(3)
          .write
          .mode("overwrite")
          .options(option)
          .format(format)
          .save(refreshTestLocation)
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_FULL)
        val newIndexLocation = s"$systemPath/index_$format"
        indexCount = spark.read
          .parquet(newIndexLocation +
            s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")
          .count()

        // Check if index got updated
        assert(indexCount == 3)

        // Check if lastest log file is updated with newly created index files
        val indexPath = PathUtils.makeAbsolute(newIndexLocation)
        val logManager = IndexLogManagerFactoryImpl.create(indexPath)
        val latestLog = logManager.getLatestLog()
        assert(latestLog.isDefined && latestLog.get.isInstanceOf[IndexLogEntry])
        val indexLog = latestLog.get.asInstanceOf[IndexLogEntry]
        val indexFiles = indexLog.content.files
        assert(indexFiles.nonEmpty)
        assert(indexFiles.forall(_.getName.startsWith("part-0")))
        assert(indexLog.state.equals("ACTIVE"))

        FileUtils.delete(new Path(refreshTestLocation))
      case _ => fail("invalid test")
    }
  }

  test("Verify incremental refresh (append-only) should index only newly appended data.") {
    withTempPathAsString { testPath =>
      // Setup. Create sample data and index.
      val indexConfig = IndexConfig(s"index", Seq("RGUID"), Seq("imprs"))
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.parquet(testPath)
      hyperspace.createIndex(df, indexConfig)
      var indexCount =
        spark.read
          .parquet(s"$systemPath/index" +
            s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
          .count()
      assert(indexCount == 10)
      // Check if latest log file is updated with newly created index files.
      validateMetadata("index", Set("v__=0"))

      // Change original data.
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
      indexCount = spark.read
        .parquet(s"$systemPath/index" +
          s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")
        .count()

      // Check if index got updated.
      assert(indexCount == 3)

      // Check if latest log file is updated with newly created index files.
      validateMetadata("index", Set("v__=0", "v__=1"))
    }
  }

  test("Verify quick optimize rebuild of index after index incremental refresh.") {
    withTempPathAsString { testPath =>
      val indexConfig = IndexConfig(s"index", Seq("RGUID"), Seq("imprs"))
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.parquet(testPath)
      hyperspace.createIndex(df, indexConfig)
      val indexCount = spark.read
        .parquet(s"$systemPath/index/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
        .count()
      assert(indexCount == 10)

      // Change original data.
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
      val newIndexCount = spark.read
        .parquet(s"$systemPath/index/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")
        .count()

      // Check if index got updated.
      assert(newIndexCount == 3)

      hyperspace.optimizeIndex(indexConfig.indexName)

      // Check if latest log file is updated with newly created index files.
      validateMetadata("index", Set("v__=2"))
    }
  }

  test("Verify optimize is a no-op if no small files found.") {
    withTempPathAsString { testPath =>
      withSQLConf(OPTIMIZE_FILE_SIZE_THRESHOLD -> "1") {
        val indexConfig = IndexConfig(s"index", Seq("RGUID"), Seq("imprs"))
        import spark.implicits._
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(10)
          .write
          .parquet(testPath)
        val df = spark.read.parquet(testPath)
        hyperspace.createIndex(df, indexConfig)

        // Change original data.
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(3)
          .write
          .mode("append")
          .parquet(testPath)
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

        // Check if latest log file is updated with newly created index files.
        val latestId = logManager(systemPath, indexConfig.indexName).getLatestId().get

        MockEventLogger.reset()
        hyperspace.optimizeIndex(indexConfig.indexName)

        // Check that no new log files were created in this operation.
        assert(latestId == logManager(systemPath, indexConfig.indexName).getLatestId().get)

        // Check emitted events.
        MockEventLogger.emittedEvents match {
          case Seq(
              OptimizeActionEvent(_, _, "Operation started."),
              OptimizeActionEvent(_, _, msg)) =>
            assert(msg.contains("Optimize aborted as no index files smaller than 1 found."))
          case _ => fail()
        }
      }
    }
  }

  private def validateMetadata(indexName: String, indexVersions: Set[String]): Unit = {
    val newIndexLocation = s"$systemPath/$indexName"
    val indexPath = PathUtils.makeAbsolute(newIndexLocation)
    val logManager = IndexLogManagerFactoryImpl.create(indexPath)
    val latestLog = logManager.getLatestLog()
    assert(latestLog.isDefined && latestLog.get.isInstanceOf[IndexLogEntry])
    val indexLog = latestLog.get.asInstanceOf[IndexLogEntry]
    val indexFiles = indexLog.content.files
    assert(indexFiles.nonEmpty)
    assert(indexFiles.forall(_.getName.startsWith("part-0")))
    assert(indexLog.state.equals("ACTIVE"))
    // Check all files belong to the provided index versions only.
    assert(indexFiles.map(_.getParent.getName).toSet.equals(indexVersions))
  }

  private def expectedIndex(
      indexConfig: IndexConfig,
      schema: StructType,
      df: DataFrame,
      state: String = Constants.States.ACTIVE): IndexLogEntry = {

    LogicalPlanSignatureProvider.create().signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val relations = df.queryExecution.optimizedPlan.collect {
          case LogicalRelation(
              HadoopFsRelation(
                location: PartitioningAwareFileIndex,
                _,
                dataSchema,
                _,
                fileFormat,
                options),
              _,
              _,
              _) =>
            val files = location.allFiles
            val sourceDataProperties = Hdfs.Properties(Content.fromLeafFiles(files))
            val fileFormatName = fileFormat match {
              case d: DataSourceRegister => d.shortName
              case other => throw HyperspaceException(s"Unsupported file format $other")
            }
            Relation(
              location.rootPaths.map(_.toString),
              Hdfs(sourceDataProperties),
              dataSchema.json,
              fileFormatName,
              options - "path")
        }
        val sourcePlanProperties = SparkPlan.Properties(
          relations,
          null,
          null,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(
              Seq(Signature(LogicalPlanSignatureProvider.create().name, s)))))

        val entry = IndexLogEntry(
          indexConfig.indexName,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(indexConfig.indexedColumns, indexConfig.includedColumns),
              IndexLogEntry.schemaString(schema),
              IndexConstants.INDEX_NUM_BUCKETS_DEFAULT)),
          Content.fromDirectory(
            PathUtils.makeAbsolute(new Path(s"$systemPath/${indexConfig.indexName}" +
              s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0"))),
          Source(SparkPlan(sourcePlanProperties)),
          Map())
        entry.state = state
        entry
      case None => fail("Invalid plan for index dataFrame.")
    }
  }

  // Verify if the indexes currently stored in Hyperspace matches the given indexes.
  private def verifyIndexes(expectedIndexes: Seq[IndexLogEntry]): Unit = {
    assert(IndexCollectionManager(spark).getIndexes().toSet == expectedIndexes.toSet)
  }
}
