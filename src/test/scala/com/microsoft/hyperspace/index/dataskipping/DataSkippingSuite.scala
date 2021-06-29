/*
 * Copyright (2021) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.index.types.dataskipping

import java.io.ByteArrayInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array_sort, collect_set, input_file_name, max, min}
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, Directory, FileIdTracker, HyperspaceSuite, IndexConstants, IndexerContext}
import com.microsoft.hyperspace.index.types.dataskipping.sketch.{BloomFilterSketch, MinMaxSketch, ValueListSketch}
import com.microsoft.hyperspace.util.FileUtils

trait DataSkippingSuite extends QueryTest with HyperspaceSuite {
  import spark.implicits._

  val dataPath = new Path(inTempDir("Data"))
  val indexDataPath = new Path(inTempDir("Index"))
  val fileNameCol = "input_file_name()"
  val emptyContent = Content(Directory.createEmptyDirectory(new Path("/")))
  val suite = this
  val ctx = new IndexerContext {
    override def spark: SparkSession = suite.spark
    override def fileIdTracker: FileIdTracker = suite.fileIdTracker
    override def indexDataPath: Path = suite.indexDataPathVar
  }

  var indexDataPathVar = indexDataPath
  var fileIdTracker: FileIdTracker = _

  before {
    indexDataPathVar = indexDataPath
    fileIdTracker = new FileIdTracker
  }

  after {
    FileUtils.delete(tempDir)
  }

  def createSourceData(
      originalData: DataFrame,
      saveMode: SaveMode = SaveMode.Overwrite): DataFrame = {
    val oldFiles = listFiles(dataPath).toSet
    originalData.write.mode(saveMode).parquet(dataPath.toString)
    updateFileIdTracker(dataPath)
    val newFiles = listFiles(dataPath).filterNot(oldFiles.contains(_))
    spark.read.parquet(newFiles.map(_.getPath.toString): _*)
  }

  def updateFileIdTracker(path: Path): Unit = {
    listFiles(path).foreach(f => fileIdTracker.addFile(f))
  }

  def withFileId(indexData: DataFrame): DataFrame = {
    val fileIdDf = fileIdTracker
      .getIdToFileMapping(_.replace("file:/", "file:///"))
      .toDF(IndexConstants.DATA_FILE_NAME_ID, fileNameCol)
    val indexDataWithFileId = indexData.join(fileIdDf, fileNameCol).drop(fileNameCol)
    val cols = indexDataWithFileId.columns
    indexDataWithFileId.select(cols.last, cols.dropRight(1): _*)
  }

  def listFiles(paths: Path*): Seq[FileStatus] = {
    val fs = paths.head.getFileSystem(new Configuration)
    fs.listStatus(paths.filter(fs.exists(_)).toArray)
  }

  def deleteFile(path: Path): Unit = {
    val fs = path.getFileSystem(new Configuration)
    fs.delete(path, true)
  }

  def isParquet: FileStatus => Boolean = _.getPath.getName.endsWith(".parquet")

  def checkBloomFilter(
      bf: BloomFilter,
      valuesMightBeContained: Seq[Any],
      valuesNotContained: Seq[Any]): Unit = {
    valuesMightBeContained.foreach { v =>
      assert(bf.mightContain(v) === true, s"bf.mightContain($v) === ${bf.mightContain(v)}")
    }
    valuesNotContained.foreach { v =>
      assert(bf.mightContain(v) === false, s"bf.mightContain($v) === ${bf.mightContain(v)}")
    }
  }
}
