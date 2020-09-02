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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper

import com.microsoft.hyperspace.{Hyperspace, SampleData}
import com.microsoft.hyperspace.util.FileUtils

class RefreshIndexDeleteTests extends HyperspaceSuite with SQLHelper {
  override val systemPath = new Path("src/test/resources/indexLocation")
  private val testDir = "src/test/resources/RefreshIndexDeleteTests/"
  private val nonPartitionedDataPath = testDir + "nonpartitioned"
  private val partitionedDataPath = testDir + "partitioned"
  private val indexConfig = IndexConfig("index1", Seq("Query"), Seq("imprs"))
  private var nonPartitionedDataDF: DataFrame = _
  private var partitionedDataDF: DataFrame = _
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sparkSession = spark
    hyperspace = new Hyperspace(sparkSession)
    FileUtils.delete(new Path(testDir))

    // save test data non-partitioned.
    SampleData.save(
      spark,
      nonPartitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))

    nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    // save test data partitioned.
    SampleData.save(
      spark,
      partitionedDataPath,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"),
      Some(Seq("Date", "Query")))

    partitionedDataDF = spark.read.parquet(partitionedDataPath)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(testDir))
    super.afterAll()
  }

  after {
    FileUtils.delete(systemPath)
  }

  test("validate refresg index when some file gets deleted.") {
    Seq(nonPartitionedDataPath, partitionedDataPath).foreach { loc =>
      withSQLConf(
        IndexConstants.INDEX_LINEAGE_ENABLED -> "true",
        IndexConstants.REFRESH_DELETE_ENABLED -> "true") {
        withIndex(indexConfig.indexName) {
          val dfToIndex =
            if (loc.equals(nonPartitionedDataPath)) nonPartitionedDataDF else partitionedDataDF
          hyperspace.createIndex(dfToIndex, indexConfig)

          // delete some source data file.
          val dataPath =
            if (loc.equals(nonPartitionedDataPath)) new Path(nonPartitionedDataPath, "*parquet")
            else new Path(partitionedDataPath + "/Date=2018-09-03/Query=ibraco", "*parquet")

          val dataFileNames = dataPath
            .getFileSystem(new Configuration)
            .globStatus(dataPath)
            .map(_.getPath)

          assert(dataFileNames.length > 0)
          val fileToDelete = dataFileNames.head
          FileUtils.delete(fileToDelete)

          val originalIndexDF = spark.read.parquet(s"$systemPath/${indexConfig.indexName}/" +
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
          val delCount =
            originalIndexDF
              .filter(s"""${IndexConstants.DATA_FILE_NAME_COLUMN} ==  "$fileToDelete"""")
              .count()

          hyperspace.refreshIndex(indexConfig.indexName)

          val refreshedIndexDF = spark.read.parquet(s"$systemPath/${indexConfig.indexName}/" +
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")

          // validate only index records whose lineage is the deleted file are removed.
          assert(refreshedIndexDF.count() == (originalIndexDF.count() - delCount))

          val lineageFileNames = refreshedIndexDF
            .select(IndexConstants.DATA_FILE_NAME_COLUMN)
            .distinct()
            .collect()
            .map(r => new Path(r.getString(0)))

          assert(!lineageFileNames.contains(fileToDelete))
        }
      }
    }
  }
}
