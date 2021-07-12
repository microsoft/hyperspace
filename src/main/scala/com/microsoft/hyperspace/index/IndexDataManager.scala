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
import org.apache.hadoop.fs.{FileSystem, Path}

import com.microsoft.hyperspace.util.FileUtils

/**
 * Index Directory Structure
 * IDRoot/
 *    _hyperspace_log/
 *        0
 *        1
 *        ...
 *        n
 *    [[IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX]]=0/
 *        f1.parquet
 *        ...
 *    [[IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX]]=1/
 *        f1.parquet
 */
trait IndexDataManager {
  def getAllFilePaths(): Seq[Path]

  def getLatestVersionId(): Option[Int]

  def getAllVersionIds(): Seq[Int]

  def getPath(id: Int): Path

  def delete(id: Int): Unit
}

class IndexDataManagerImpl(indexPath: Path, configuration: Configuration)
    extends IndexDataManager {
  // TODO: Investigate whether FileContext should be used instead of FileSystem for atomic renames.
  private lazy val fs: FileSystem = indexPath.getFileSystem(configuration)

  /**
   * Get latest version id of the index data directory.
   */
  override def getLatestVersionId(): Option[Int] = {
    val ids = getAllVersionIds()
    if (ids.isEmpty) None else Some(ids.max)
  }

  /**
   * This method relies on the naming convention that directory name will be similar to hive
   * partitioning scheme, i.e. {{{"root/v__=value/f1.parquet"}}} etc. Here the value represents the
   * version id of the data.
   */
  override def getAllVersionIds(): Seq[Int] = {
    if (!fs.exists(indexPath)) {
      return Nil
    }
    val prefixLength = IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX.length + 1
    fs.listStatus(indexPath)
      .collect {
        case status
            if status.getPath.getName.startsWith(IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX) =>
          status.getPath.getName.drop(prefixLength).toInt
      }
  }

  /**
   * Get all file paths in the index directory.
   */
  override def getAllFilePaths(): Seq[Path] = {
    if (!fs.exists(indexPath)) {
      return Nil
    }
    val directories = fs.listStatus(indexPath).collect {
      case status
          if status.getPath.getName.startsWith(IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX) =>
        status.getPath
    }
    directories.flatMap { dir =>
      fs.listStatus(dir).collect {
        case status => status.getPath
      }
    }
  }

  /**
   * Get directory path of the given id.
   */
  override def getPath(id: Int): Path = {
    new Path(indexPath, s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=${id.toString}")
  }

  override def delete(id: Int): Unit = FileUtils.delete(getPath(id))
}
