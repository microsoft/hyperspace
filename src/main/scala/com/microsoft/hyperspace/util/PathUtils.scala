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

package com.microsoft.hyperspace.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql.execution.datasources.PartitionSpec

object PathUtils {
  def makeAbsolute(path: String): Path = makeAbsolute(new Path(path))

  def makeAbsolute(path: Path): Path = {
    val fs = path.getFileSystem(new Configuration)
    fs.makeQualified(path)
  }

  /**
   * Extract base data source path for from a given partition spec.
   * @param partitionSpec PartitionSpec.
   * @return Optional base path if partition spec is non empty. Else, None.
   */
  def extractBasePath(partitionSpec: PartitionSpec): Option[String] = {
    if (partitionSpec == PartitionSpec.emptySpec) {
      None
    } else {
      // For example, we could have the following in PartitionSpec:
      //   - partition columns = "col1", "col2"
      //   - partitions: "/path/col1=1/col2=1", "/path/col1=1/col2=2", etc.
      // , and going up the same number of directory levels as the number of partition columns
      // will compute the base path. Note that PartitionSpec.partitions will always contain
      // all the partitions in the path, so "partitions.head" is taken as an initial value.
      val basePath = partitionSpec.partitionColumns
        .foldLeft(partitionSpec.partitions.head.path)((path, _) => path.getParent)
      Some(basePath.toString)
    }
  }

  /* Definition taken from org.apache.spark.sql.execution.datasources.PartitionAwareFileIndex. */
  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  object DataPathFilter extends PathFilter {
    override def accept(path: Path): Boolean = {
      val name = path.getName
      !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
    }
  }
}
