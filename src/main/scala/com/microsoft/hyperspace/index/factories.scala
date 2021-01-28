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

trait IndexLogManagerFactory {
  def create(indexPath: Path, configuration: Configuration): IndexLogManager
}

object IndexLogManagerFactoryImpl extends IndexLogManagerFactory {
  override def create(
      indexPath: Path,
      configuration: Configuration = new Configuration): IndexLogManager = {
    new IndexLogManagerImpl(indexPath, configuration)
  }
}

trait IndexDataManagerFactory {
  def create(indexPath: Path, configuration: Configuration): IndexDataManager
}

object IndexDataManagerFactoryImpl extends IndexDataManagerFactory {
  override def create(
      indexPath: Path,
      configuration: Configuration = new Configuration): IndexDataManager = {
    new IndexDataManagerImpl(indexPath, configuration)
  }
}

trait FileSystemFactory {
  def create(path: Path, configuration: Configuration): FileSystem
}

object FileSystemFactoryImpl extends FileSystemFactory {
  override def create(path: Path, configuration: Configuration = new Configuration): FileSystem =
    path.getFileSystem(configuration)
}
