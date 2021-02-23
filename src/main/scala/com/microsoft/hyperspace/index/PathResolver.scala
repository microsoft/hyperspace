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

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.internal.SQLConf

/**
 * Getter function to retrieve Hyperspace index related directory paths from spark config.
 *
 * @param conf SQL Configuration
 */
private[hyperspace] class PathResolver(conf: SQLConf, hadoopConf: Configuration) {

  /**
   * Get path for the given index name. It enumerates the file system to resolve
   * case sensitivity - it matches the existing index name in a case-insensitive way.
   *
   * @param name index name
   * @return resolved index path
   */
  def getIndexPath(name: String): Path = {
    val root = systemPath
    val fs = root.getFileSystem(hadoopConf)
    if (fs.exists(root)) {
      // Note that fs.exists() is case-sensitive in some platforms and case-insensitive
      // in others, thus the check is manually done to make the comparison case-insensitive.
      val indexNames = fs.listStatus(root)
      indexNames
        .collectFirst {
          case s: FileStatus
              if s.getPath.getName
                .toLowerCase(Locale.ROOT)
                .equals(name.toLowerCase(Locale.ROOT)) =>
            s.getPath
        }
        .getOrElse(new Path(root, name))
    } else {
      new Path(root, name)
    }
  }

  /**
   * Get the Hyperspace index system path.
   *
   * @return Hyperspace index system path.
   */
  def systemPath: Path = {
    val defaultIndexesPath =
      new Path(conf.getConfString("spark.sql.warehouse.dir"), "indexes")
    new Path(conf.getConfString(IndexConstants.INDEX_SYSTEM_PATH, defaultIndexesPath.toString))
  }
}

object PathResolver {
  def apply(conf: SQLConf, hadoopConf: Configuration): PathResolver = {
    new PathResolver(conf, hadoopConf)
  }
}
