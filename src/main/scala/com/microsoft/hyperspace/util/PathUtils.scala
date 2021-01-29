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

object PathUtils {
  def makeAbsolute(path: String, hadoopConfiguration: Configuration = new Configuration): Path =
    makeAbsolute(new Path(path), hadoopConfiguration)

  def makeAbsolute(path: Path, hadoopConfiguration: Configuration): Path = {
    val fs = path.getFileSystem(hadoopConfiguration)
    fs.makeQualified(path)
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
