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

package com.microsoft.hyperspace

import org.apache.hadoop.fs

import com.microsoft.hyperspace.index.IndexLogEntry

object TestUtils {
  def copyWithState(index: IndexLogEntry, state: String): IndexLogEntry = {
    val result = index.copy()
    result.state = state
    result
  }

  /**
   * Split path into its segments and return segment names as a sequence.
   * For e.g. a path "file:/C:/d1/d2/d3/f1.parquet" will return
   * Seq("file:/C:/", "d1", "d2", "d3", "f1.parquet")
   *
   * @param path Path to split into segments.
   * @return Segments as a seq.
   */
  def splitPath(path: fs.Path): Seq[String] = {
    var initialPath = path
    var splits = Seq[String]()
    while (initialPath.getParent != null) {
      splits :+= initialPath.getName
      initialPath = initialPath.getParent
    }
    // Initial path is now root. It's getName returns "" but toString returns actual path,
    // E.g. "file:/C:/".
    splits :+ initialPath.toString
  }
}
