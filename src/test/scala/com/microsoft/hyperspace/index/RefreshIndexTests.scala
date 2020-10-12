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
import org.apache.spark.sql.{AnalysisException, QueryTest}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, MockEventLogger, SampleData}
import com.microsoft.hyperspace.telemetry.RefreshDeleteActionEvent
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

/**
 * Unit E2E test cases for RefreshIndex.
 */
class RefreshIndexTests extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/indexLocation")
  private val testDir = "src/test/resources/RefreshIndexDeleteTests/"
  private val nonPartitionedDataPath = testDir + "nonpartitioned"
  private val partitionedDataPath = testDir + "partitioned"
  private val indexConfig = IndexConfig("index1", Seq("Query"), Seq("imprs"))
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(testDir))
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(testDir))
    super.afterAll()
  }

  after {
    FileUtils.delete(new Path(testDir))
    FileUtils.delete(systemPath)
  }

  test("Validate refresh index when some file gets deleted from the source data.") {
    // Save test data non-partitioned.
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    // Save test data partitioned.
    SampleData.save(
      spark,
      partitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"),
      Some(Seq("Date", "Query")))
    val partitionedDataDF = spark.read.parquet(partitionedDataPath)

    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      withSQLConf(
        IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
        IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          val dfToIndex =
            if (loc.equals(nonPartitionedDataPath)) nonPartitionedDataDF else partitionedDataDF
          hyperspace.createIndex(dfToIndex, indexConfig)

          // Delete one source data file.
          val deletedFile = if (loc.equals(nonPartitionedDataPath)) {
            deleteDataFile(nonPartitionedDataPath)
          } else {
            deleteDataFile(partitionedDataPath, true)
          }

          // Validate only index records whose lineage is the deleted file are removed.
          val originalIndexDF = spark.read.parquet(s"$systemPath/${indexConfig.indexName}/" +
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
          val originalIndexWithoutDeletedFile = originalIndexDF
            .filter(s"""${IndexConstants.DATA_FILE_NAME_COLUMN} != "$deletedFile"""")

          hyperspace.refreshIndex(indexConfig.indexName)

          val refreshedIndexDF = spark.read.parquet(s"$systemPath/${indexConfig.indexName}/" +
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")

          checkAnswer(originalIndexWithoutDeletedFile, refreshedIndexDF)
        }
      }
    }
  }

  test(
    "Validate refresh index (to handle deletes from the source data) " +
      "fails as expected on an index without lineage.") {
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(
      IndexConstants.INDEX_LINEAGE_ENABLED -> "false",
      IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

      deleteDataFile(nonPartitionedDataPath)

      val ex = intercept[HyperspaceException](hyperspace.refreshIndex(indexConfig.indexName))
      assert(
        ex.getMessage.contains(s"Index refresh (to handle deleted source data) is " +
          "only supported on an index with lineage."))
    }
  }

  test(
    "Validate refresh index (to handle deletes from the source data) " +
      "is a no-op if no source data file is deleted.") {
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(
      IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
      IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

      val latestId = logManager(indexConfig.indexName).getLatestId().get

      MockEventLogger.reset()
      hyperspace.refreshIndex(indexConfig.indexName)
      // Check that no new log files were created in this operation.
      assert(latestId == logManager(indexConfig.indexName).getLatestId().get)

      // Check emitted events.
      MockEventLogger.emittedEvents match {
        case Seq(
            RefreshDeleteActionEvent(_, _, "Operation started."),
            RefreshDeleteActionEvent(_, _, msg)) =>
          assert(msg.contains("Refresh delete aborted as no deleted source data file found."))
        case _ => fail()
      }
    }
  }

  test(
    "Validate refresh index (to handle deletes from the source data) " +
      "fails as expected when all source data files are deleted.") {
    Seq(true, false).foreach { deleteDataFolder =>
      withSQLConf(
        IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
        IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
        SampleData.save(
          spark,
          nonPartitionedDataPath,
          Seq("Date", "RGUID", "Query", "imprs", "clicks"))
        val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

        hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

        if (deleteDataFolder) {
          FileUtils.delete(new Path(nonPartitionedDataPath))

          val ex = intercept[AnalysisException](hyperspace.refreshIndex(indexConfig.indexName))
          assert(ex.getMessage.contains("Path does not exist"))

        } else {
          val dataPath = new Path(nonPartitionedDataPath, "*parquet")
          dataPath
            .getFileSystem(new Configuration)
            .globStatus(dataPath)
            .foreach(p => FileUtils.delete(p.getPath))

          val ex =
            intercept[HyperspaceException](hyperspace.refreshIndex(indexConfig.indexName))
          assert(ex.getMessage.contains("Invalid plan for creating an index."))
        }
        FileUtils.delete(new Path(nonPartitionedDataPath))
        FileUtils.delete(systemPath)
      }
    }
  }

  test(
    "Validate refresh index (to handle deletes from the source data) " +
      "fails as expected when file info for an existing source data file changes.") {
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(
      IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
      IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

      // Replace a source data file with a new file with same name but different properties.
      val deletedFile = deleteDataFile(nonPartitionedDataPath)
      FileUtils.createFile(
        deletedFile.getFileSystem(new Configuration),
        deletedFile,
        "I am some random content :).")

      val ex = intercept[HyperspaceException](hyperspace.refreshIndex(indexConfig.indexName))
      assert(
        ex.getMessage.contains("Index refresh (to handle deleted source data) aborted. " +
          "Existing source data file info is changed"))
    }
  }

  test(
    "Validate refresh delete action updates appended and deleted files in metadata as " +
      "expected, when some file gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(
        IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
        IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          SampleData.save(
            spark,
            testPath,
            Seq("Date", "RGUID", "Query", "imprs", "clicks"))
          val df = spark.read.parquet(testPath)
          hyperspace.createIndex(df, indexConfig)

          // Delete one source data file.
          deleteDataFile(testPath)

          val oldFiles = listFiles(testPath).toSet

          // Add some new data to source.
          import spark.implicits._
          SampleData.testData
            .take(3)
            .toDF("Date", "RGUID", "Query", "imprs", "clicks")
            .write
            .mode("append")
            .parquet(testPath)

          hyperspace.refreshIndex(indexConfig.indexName)

          // Verify "deletedFiles" is cleared and "appendedFiles" is updated after refresh.
          val indexLogEntry = getLatestStableLog(indexConfig.indexName)
          val latestFiles = listFiles(testPath).toSet

          assert(indexLogEntry.deletedFiles.isEmpty)
          assert((oldFiles -- latestFiles).isEmpty)
          assert(indexLogEntry.appendedFiles.toSet.equals(latestFiles -- oldFiles))
        }
      }
    }
  }

  test(
    "Validate refresh append action updates appended and deleted files in metadata as" +
      "expected, when some file gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.REFRESH_APPEND_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          SampleData.save(spark, testPath, Seq("Date", "RGUID", "Query", "imprs", "clicks"))
          val df = spark.read.parquet(testPath)
          hyperspace.createIndex(df, indexConfig)

          val oldFiles = listFiles(testPath).toSet

          // Delete one source data file.
          deleteDataFile(testPath)

          // Add some new data to source.
          import spark.implicits._
          SampleData.testData
            .take(3)
            .toDF("Date", "RGUID", "Query", "imprs", "clicks")
            .write
            .mode("append")
            .parquet(testPath)

          hyperspace.refreshIndex(indexConfig.indexName)

          // Verify "appendedFiles" is cleared and "deletedFiles" is updated after refresh.
          val indexLogEntry = getLatestStableLog(indexConfig.indexName)
          assert(indexLogEntry.appendedFiles.isEmpty)

          val latestFiles = listFiles(testPath).toSet
          assert(indexLogEntry.deletedFiles.toSet.equals(oldFiles -- latestFiles))
        }
      }
    }
  }

  /**
   * Delete one file from a given path.
   *
   * @param path Path to the parent folder containing data files.
   * @param isPartitioned Is data folder partitioned or not.
   * @return Path to the deleted file.
   */
  private def deleteDataFile(path: String, isPartitioned: Boolean = false): Path = {
    val dataPath = if (isPartitioned) {
      new Path(s"$path/*/*", "*parquet")
    } else {
      new Path(path, "*parquet")
    }

    val dataFileNames = dataPath
      .getFileSystem(new Configuration)
      .globStatus(dataPath)
      .map(_.getPath)

    assert(dataFileNames.nonEmpty)
    val fileToDelete = dataFileNames.head
    FileUtils.delete(fileToDelete)

    fileToDelete
  }

  private def logManager(indexName: String): IndexLogManager = {
    val indexPath = PathUtils.makeAbsolute(s"$systemPath/$indexName")
    IndexLogManagerFactoryImpl.create(indexPath)
  }

  private def listFiles(path: String): Seq[String] = {
    val absolutePath = PathUtils.makeAbsolute(path)
    val fs = absolutePath.getFileSystem(new Configuration)
    fs.listStatus(absolutePath).toSeq.map(_.getPath.toString)
  }

  private def getLatestStableLog(indexName: String): IndexLogEntry = {
    val entry = logManager(indexName).getLatestStableLog()
    assert(entry.isDefined)
    assert(entry.get.isInstanceOf[IndexLogEntry])
    entry.get.asInstanceOf[IndexLogEntry]
  }
}
