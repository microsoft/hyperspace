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

import com.microsoft.hyperspace.util.PathUtils

trait IndexLogManagerFactory {
  def create(indexPath: Path): IndexLogManager
}

object IndexLogManagerFactoryImpl extends IndexLogManagerFactory {
  override def create(indexPath: Path): IndexLogManager = {
    new IndexLogManagerImpl(PathUtils.makeAbsolute(indexPath))
  }
}

trait IndexDataManagerFactory {
  def create(indexPath: Path): IndexDataManager
}

object IndexDataManagerFactoryImpl extends IndexDataManagerFactory {
  override def create(indexPath: Path): IndexDataManager = {
    new IndexDataManagerImpl(PathUtils.makeAbsolute(indexPath))
  }
}

trait FileSystemFactory {
  def create(path: Path): FileSystem
}

object FileSystemFactoryImpl extends FileSystemFactory {
  private val configuration = new Configuration

  override def create(path: Path): FileSystem = path.getFileSystem(configuration)
}
