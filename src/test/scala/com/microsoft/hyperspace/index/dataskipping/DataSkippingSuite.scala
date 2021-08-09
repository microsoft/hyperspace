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

package com.microsoft.hyperspace.index.dataskipping

import scala.collection.AbstractIterator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode, SparkSession}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.FileUtils

trait DataSkippingSuite extends QueryTest with HyperspaceSuite {
  import spark.implicits._

  val dataPathRoot = new Path(inTempDir("Data"))
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
  var hs: Hyperspace = _

  before {
    indexDataPathVar = indexDataPath
    fileIdTracker = new FileIdTracker
    hs = new Hyperspace(spark)
  }

  after {
    FileUtils.delete(tempDir)
  }

  def dataPath(path: String = "T"): Path = new Path(dataPathRoot, path)

  def createSourceData(
      originalData: DataFrame,
      path: String = "T",
      saveMode: SaveMode = SaveMode.Overwrite,
      appendedDataOnly: Boolean = false): DataFrame = {
    val p = dataPath(path)
    val oldFiles = listFiles(p).toSet
    originalData.write.mode(saveMode).parquet(p.toString)
    updateFileIdTracker(p)
    if (appendedDataOnly) {
      val newFiles = listFiles(p).filterNot(oldFiles.contains)
      spark.read.parquet(newFiles.map(_.getPath.toString): _*)
    } else {
      spark.read.parquet(p.toString)
    }
  }

  def updateFileIdTracker(path: Path): Unit = {
    listFiles(path).foreach(f => fileIdTracker.addFile(f))
  }

  def withFileId(indexData: DataFrame): DataFrame = {
    val fileIdDf = fileIdTracker
      .getIdToFileMapping(_.replace("file:/", "file:///"))
      .toDF(IndexConstants.DATA_FILE_NAME_ID, fileNameCol)
    indexData
      .join(
        fileIdDf,
        IndexUtils.decodeInputFileName(indexData(fileNameCol)) === fileIdDf(fileNameCol))
      .select(
        IndexConstants.DATA_FILE_NAME_ID,
        indexData.columns.filterNot(_ == fileNameCol).map(c => s"`$c`"): _*)
  }

  def listFiles(paths: Path*): Seq[FileStatus] = {
    val fs = paths.head.getFileSystem(new Configuration)
    paths
      .filter(fs.exists)
      .flatMap { p =>
        val it = fs.listFiles(p, true)
        case class IteratorAdapter[T](it: RemoteIterator[T]) extends AbstractIterator[T] {
          def hasNext: Boolean = it.hasNext
          def next(): T = it.next()
        }
        IteratorAdapter(it).toSeq
      }
      .sortBy(_.getPath.toString)
  }

  def deleteFile(path: Path): Unit = {
    val fs = path.getFileSystem(new Configuration)
    fs.delete(path, true)
  }

  def isParquet: FileStatus => Boolean = _.getPath.getName.endsWith(".parquet")
}
