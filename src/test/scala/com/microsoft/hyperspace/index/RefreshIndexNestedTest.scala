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
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{AnalysisException, QueryTest}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, MockEventLogger, SampleNestedData, TestUtils}
import com.microsoft.hyperspace.TestUtils.{getFileIdTracker, logManager}
import com.microsoft.hyperspace.actions.{RefreshIncrementalAction, RefreshQuickAction}
import com.microsoft.hyperspace.index.IndexConstants.REFRESH_MODE_INCREMENTAL
import com.microsoft.hyperspace.telemetry.RefreshIncrementalActionEvent
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}
import com.microsoft.hyperspace.util.PathUtils.DataPathFilter

/**
 * Unit E2E test cases for RefreshIndex.
 */
class RefreshIndexNestedTest extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/indexLocation")
  private val testDir = "src/test/resources/RefreshIndexDeleteTests/"
  private val nonPartitionedDataPath = testDir + "nonpartitioned"
  private val partitionedDataPath = testDir + "partitioned"
  private val indexConfig = IndexConfig("index1", Seq("nested.leaf.id"), Seq("nested.leaf.cnt"))
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

  test("Validate incremental refresh index when some file gets deleted from the source data.") {
    // Save test data non-partitioned.
    SampleNestedData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    // Save test data partitioned.
    SampleNestedData.save(
      spark,
      partitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"),
      Some(Seq("Date", "Query")))
    val partitionedDataDF = spark.read.parquet(partitionedDataPath)

    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          val dfToIndex =
            if (loc.equals(nonPartitionedDataPath)) nonPartitionedDataDF else partitionedDataDF
          hyperspace.createIndex(dfToIndex, indexConfig)

          // Delete one source data file.
          val deletedFile = if (loc.equals(nonPartitionedDataPath)) {
            deleteOneDataFile(nonPartitionedDataPath)
          } else {
            deleteOneDataFile(partitionedDataPath, true)
          }

          // Get deleted file's file id, used as lineage for its records.
          val fileId = getFileIdTracker(systemPath, indexConfig).getFileId(
            deletedFile.getPath.toString,
            deletedFile.getLen,
            deletedFile.getModificationTime)
          assert(fileId.nonEmpty)

          // Validate only index records whose lineage is the deleted file are removed.
          val originalIndexDF = spark.read.parquet(s"$systemPath/${indexConfig.indexName}/" +
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
          val originalIndexWithoutDeletedFile = originalIndexDF
            .filter(s"""${IndexConstants.DATA_FILE_NAME_ID} != ${fileId.get}""")

          hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

          val refreshedIndexDF = spark.read.parquet(s"$systemPath/${indexConfig.indexName}/" +
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")

          checkAnswer(originalIndexWithoutDeletedFile, refreshedIndexDF)
        }
      }
    }
  }

  test(
    "Validate incremental refresh index (to handle deletes from the source data) " +
      "fails as expected on an index without lineage.") {
    SampleNestedData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "false") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

      deleteOneDataFile(nonPartitionedDataPath)

      val ex = intercept[HyperspaceException](
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL))
      assert(
        ex.getMessage.contains(s"Index refresh (to handle deleted source data) is " +
          "only supported on an index with lineage."))
    }
  }

  test(
    "Validate incremental refresh index is a no-op if no source data file is deleted or " +
      "appended.") {
    SampleNestedData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

      val latestId = logManager(systemPath, indexConfig.indexName).getLatestId().get

      MockEventLogger.reset()
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
      // Check that no new log files were created in this operation.
      assert(latestId == logManager(systemPath, indexConfig.indexName).getLatestId().get)

      // Check emitted events.
      MockEventLogger.emittedEvents match {
        case Seq(
            RefreshIncrementalActionEvent(_, _, "Operation started."),
            RefreshIncrementalActionEvent(_, _, msg)) =>
          assert(msg.contains("Refresh incremental aborted as no source data change found."))
        case _ => fail()
      }
    }
  }

  test(
    "Validate incremental refresh index (to handle deletes from the source data) " +
      "fails as expected when all source data files are deleted.") {
    Seq(true, false).foreach { deleteDataFolder =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        SampleNestedData.save(
          spark,
          nonPartitionedDataPath,
          Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
        val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

        hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

        if (deleteDataFolder) {
          FileUtils.delete(new Path(nonPartitionedDataPath))

          val ex = intercept[AnalysisException](
            hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL))
          assert(ex.getMessage.contains("Path does not exist"))

        } else {
          val dataPath = new Path(nonPartitionedDataPath, "*parquet")
          dataPath
            .getFileSystem(new Configuration)
            .globStatus(dataPath)
            .foreach(p => FileUtils.delete(p.getPath))

          val ex =
            intercept[HyperspaceException](
              hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL))
          assert(ex.getMessage.contains("Invalid plan for creating an index."))
        }
        FileUtils.delete(new Path(nonPartitionedDataPath))
        FileUtils.delete(systemPath)
      }
    }
  }

  test(
    "Validate incremental refresh index (to handle deletes from the source data) " +
      "works well when file info for an existing source data file changes.") {
    SampleNestedData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)
    }

    // Replace a source data file with a new file with same name but different properties.
    val deletedFile = deleteOneDataFile(nonPartitionedDataPath)
    val sourcePath = new Path(spark.read.parquet(nonPartitionedDataPath).inputFiles.head)
    val fs = deletedFile.getPath.getFileSystem(new Configuration)
    fs.copyToLocalFile(sourcePath, deletedFile.getPath)

    {
      // Check the index log entry before refresh.
      val indexLogEntry = getLatestStableLog(indexConfig.indexName)
      assert(logManager(systemPath, indexConfig.indexName).getLatestId().get == 1)
      assert(getIndexFilesCount(indexLogEntry, version = 0) == indexLogEntry.content.files.size)
    }

    val indexPath = PathUtils.makeAbsolute(s"$systemPath/${indexConfig.indexName}")
    new RefreshIncrementalAction(
      spark,
      IndexLogManagerFactoryImpl.create(indexPath),
      IndexDataManagerFactoryImpl.create(indexPath))
      .run()

    {
      // Check the index log entry after RefreshIncrementalAction.
      val indexLogEntry = getLatestStableLog(indexConfig.indexName)
      assert(logManager(systemPath, indexConfig.indexName).getLatestId().get == 3)
      assert(indexLogEntry.deletedFiles.isEmpty)
      assert(indexLogEntry.appendedFiles.isEmpty)

      val files = indexLogEntry.relations.head.data.properties.content.files
      assert(files.exists(_.equals(deletedFile.getPath)))
      assert(
        getIndexFilesCount(indexLogEntry, version = 1) == indexLogEntry.content.fileInfos.size)
    }

    // Modify the file again.
    val sourcePath2 = new Path(spark.read.parquet(nonPartitionedDataPath).inputFiles.last)
    fs.copyToLocalFile(sourcePath2, deletedFile.getPath)

    new RefreshIncrementalAction(
      spark,
      IndexLogManagerFactoryImpl.create(indexPath),
      IndexDataManagerFactoryImpl.create(indexPath))
      .run()

    {
      // Check non-empty deletedFiles after RefreshIncrementalAction.
      val indexLogEntry = getLatestStableLog(indexConfig.indexName)
      assert(indexLogEntry.deletedFiles.isEmpty)
      assert(indexLogEntry.appendedFiles.isEmpty)
      assert(logManager(systemPath, indexConfig.indexName).getLatestId().get == 5)
      val files = indexLogEntry.relations.head.data.properties.content.files
      assert(files.exists(_.equals(deletedFile.getPath)))
      assert(
        getIndexFilesCount(indexLogEntry, version = 2) == indexLogEntry.content.fileInfos.size)
    }
  }

  test(
    "Validate RefreshIncrementalAction updates appended and deleted files in metadata " +
      "as expected, when some file gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          SampleNestedData.save(spark, testPath,
            Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
          val df = spark.read.parquet(testPath)
          hyperspace.createIndex(df, indexConfig)

          val oldFiles = listFiles(testPath, getFileIdTracker(systemPath, indexConfig)).toSet

          // Delete one source data file.
          deleteOneDataFile(testPath)

          // Add some new data to source.
          import spark.implicits._
          SampleNestedData.testData
            .take(3)
            .toDF("Date", "RGUID", "Query", "imprs", "clicks", "nested")
            .write
            .mode("append")
            .parquet(testPath)

          val indexPath = PathUtils.makeAbsolute(s"$systemPath/${indexConfig.indexName}")
          new RefreshIncrementalAction(
            spark,
            IndexLogManagerFactoryImpl.create(indexPath),
            IndexDataManagerFactoryImpl.create(indexPath))
            .run()

          // Verify "appendedFiles" is cleared and "deletedFiles" is updated after refresh.
          val indexLogEntry = getLatestStableLog(indexConfig.indexName)
          assert(indexLogEntry.appendedFiles.isEmpty)

          val latestFiles = listFiles(testPath, getFileIdTracker(systemPath, indexConfig)).toSet
          val indexSourceFiles = indexLogEntry.relations.head.data.properties.content.fileInfos
          val expectedDeletedFiles = oldFiles -- latestFiles
          val expectedAppendedFiles = latestFiles -- oldFiles
          assert(expectedDeletedFiles.forall(f => !indexSourceFiles.contains(f)))
          assert(expectedAppendedFiles.forall(indexSourceFiles.contains))
          assert(indexSourceFiles.forall(f =>
            expectedAppendedFiles.contains(f) || oldFiles.contains(f)))
        }
      }
    }
  }

  test(
    "Validate incremental refresh index when some file gets deleted and some appended to " +
      "source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          // Save test data non-partitioned.
          SampleNestedData.save(spark, testPath,
            Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
          val df = spark.read.parquet(testPath)
          hyperspace.createIndex(df, indexConfig)
          val countOriginal = df.count()

          // Delete one source data file.
          deleteOneDataFile(testPath)
          val countAfterDelete = spark.read.parquet(testPath).count()
          assert(countAfterDelete < countOriginal)

          // Add some new data to source.
          import spark.implicits._
          SampleNestedData.testData
            .take(3)
            .toDF("Date", "RGUID", "Query", "imprs", "clicks", "nested")
            .write
            .mode("append")
            .parquet(testPath)

          val countAfterAppend = spark.read.parquet(testPath).count()
          assert(countAfterDelete + 3 == countAfterAppend)

          hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

          // Check if refreshed index is updated appropriately.
          val indexDf = spark.read
            .parquet(s"$systemPath/${indexConfig.indexName}/" +
              s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")

          assert(indexDf.count() == countAfterAppend)
        }
      }
    }
  }

  test(
    "Validate the configs for incremental index data is consistent with" +
      "the previous version.") {
    withTempPathAsString { testPath =>
      SampleNestedData.save(spark, testPath,
        Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
      val df = spark.read.parquet(testPath)

      withSQLConf(
        IndexConstants.INDEX_LINEAGE_ENABLED -> "false",
        IndexConstants.INDEX_NUM_BUCKETS -> "20") {
        hyperspace.createIndex(df, indexConfig)
      }

      // Add some new data to source.
      import spark.implicits._
      SampleNestedData.testData
        .take(3)
        .toDF("Date", "RGUID", "Query", "imprs", "clicks", "nested")
        .write
        .mode("append")
        .parquet(testPath)

      withSQLConf(
        IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
        IndexConstants.INDEX_NUM_BUCKETS -> "10") {
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
      }

      val indexLogEntry = getLatestStableLog(indexConfig.indexName)
      assert(!indexLogEntry.hasLineageColumn)
      assert(indexLogEntry.numBuckets === 20)
    }
  }

  test(
    "Validate RefreshQuickAction updates appended and deleted files in metadata " +
      "as expected, when some file gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          SampleNestedData.save(spark, testPath,
            Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested"))
          val df = spark.read.parquet(testPath)
          hyperspace.createIndex(df, indexConfig)

          val oldFiles = listFiles(testPath, getFileIdTracker(systemPath, indexConfig)).toSet

          // Delete one source data file.
          deleteOneDataFile(testPath)

          // Add some new data to source.
          import spark.implicits._
          SampleNestedData.testData
            .take(3)
            .toDF("Date", "RGUID", "Query", "imprs", "clicks", "nested")
            .write
            .mode("append")
            .parquet(testPath)

          val prevIndexLogEntry = getLatestStableLog(indexConfig.indexName)

          val indexPath = PathUtils.makeAbsolute(s"$systemPath/${indexConfig.indexName}")
          new RefreshQuickAction(
            spark,
            IndexLogManagerFactoryImpl.create(indexPath),
            IndexDataManagerFactoryImpl.create(indexPath))
            .run()

          val indexLogEntry = getLatestStableLog(indexConfig.indexName)
          val latestFiles = listFiles(testPath, getFileIdTracker(systemPath, indexConfig)).toSet
          val expectedDeletedFiles = oldFiles -- latestFiles
          val expectedAppendedFiles = latestFiles -- oldFiles

          val signatureProvider = LogicalPlanSignatureProvider.create()
          val latestDf = spark.read.parquet(testPath)
          val expectedLatestSignature =
            signatureProvider.signature(latestDf.queryExecution.optimizedPlan).get

          // Check `Update` is collected properly.
          assert(indexLogEntry.sourceUpdate.isDefined)
          assert(
            indexLogEntry.source.plan.properties.fingerprint.properties.signatures.head.value
              == expectedLatestSignature)
          assert(indexLogEntry.appendedFiles === expectedAppendedFiles)
          assert(indexLogEntry.deletedFiles === expectedDeletedFiles)

          // Check index data files and source data files are not updated.
          assert(
            indexLogEntry.relations.head.data.properties.content.fileInfos
              === prevIndexLogEntry.relations.head.data.properties.content.fileInfos)
          assert(indexLogEntry.content.fileInfos === prevIndexLogEntry.content.fileInfos)
        }
      }
    }
  }

  /**
   * Delete one file from a given path.
   *
   * @param path Path to the parent folder containing data files.
   * @param isPartitioned Is data folder partitioned or not.
   * @return Deleted file's FileStatus.
   */
  private def deleteOneDataFile(path: String, isPartitioned: Boolean = false): FileStatus = {
    val dataPath = if (isPartitioned) s"$path/*/*" else path
    TestUtils.deleteFiles(dataPath, "*parquet", 1).head
  }

  private def listFiles(path: String, fileIdTracker: FileIdTracker): Seq[FileInfo] = {
    val absolutePath = PathUtils.makeAbsolute(path)
    val fs = absolutePath.getFileSystem(new Configuration)
    fs.listStatus(absolutePath)
      .toSeq
      .filter(f => DataPathFilter.accept(f.getPath))
      .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = true))
  }

  private def getLatestStableLog(indexName: String): IndexLogEntry = {
    val entry = logManager(systemPath, indexName).getLatestStableLog()
    assert(entry.isDefined)
    assert(entry.get.isInstanceOf[IndexLogEntry])
    entry.get.asInstanceOf[IndexLogEntry]
  }

  private def getIndexFilesCount(
      entry: IndexLogEntry,
      version: Int,
      allowEmpty: Boolean = false) = {
    val cnt = entry.content.fileInfos
      .count(_.name.contains(s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=$version"))
    assert(allowEmpty || cnt > 0)
    cnt
  }

}
