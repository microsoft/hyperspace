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

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, SampleData}
import com.microsoft.hyperspace.util.FileUtils

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
      "is aborted if no source data file is deleted.") {
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(
      IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
      IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig)

      val ex = intercept[HyperspaceException](hyperspace.refreshIndex(indexConfig.indexName))
      assert(ex.getMessage.contains("Refresh aborted as no deleted source data file found."))
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
    "Validate refresh index (to handle deletes from the source data) " +
      "for enforce delete on read.") {
    // Save test data non-partitioned.
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    val nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    withSQLConf(
      IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
      IndexConstants.ENFORCE_DELETE_ON_READ_ENABLED -> "true") {
      withIndex(indexConfig.indexName) {
        hyperspace.createIndex(nonPartitionedDataDF, indexConfig)
        val ixManager = Hyperspace.getContext(spark).indexCollectionManager
        val originalIndex = ixManager.getIndexes()
        assert(originalIndex.length == 1)
        assert(
          originalIndex.head.source.plan.properties.relations.head.data.properties.excluded.isEmpty)

        // Delete one source data file.
        val deletedFile = deleteDataFile(nonPartitionedDataPath)

        hyperspace.refreshIndex(indexConfig.indexName)
        val refreshedIndex = ixManager.getIndexes()
        assert(refreshedIndex.length == 1)
        assert(
          refreshedIndex.head.source.plan.properties.relations.head.data.properties.excluded
            .equals(Seq(deletedFile.toString)))

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
}
