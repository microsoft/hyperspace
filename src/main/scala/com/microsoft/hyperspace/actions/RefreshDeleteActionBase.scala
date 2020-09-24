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

package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, IndexDataManager, IndexLogManager}

private[actions] abstract class RefreshDeleteActionBase(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {

  /**
   * Validate index has lineage column and it is in active state for refreshing and
   * there are some deleted source data file(s).
   */
  override def validate(): Unit = {
    super.validate()
    if (!previousIndexLogEntry.hasLineageColumn(spark)) {
      throw HyperspaceException(
        "Index refresh (to handle deleted source data) is " +
          "only supported on an index with lineage.")
    }

    if (deletedFiles.isEmpty || deletedFiles.toSet.equals(
          previousIndexLogEntry.excludedFiles.toSet)) {
      throw HyperspaceException("Refresh aborted as no deleted source data file found.")
    }
  }

  /**
   * Compare list of source data files from previous IndexLogEntry to list
   * of current source data files, validate fileInfo for existing files and
   * identify deleted source data files.
   */
  protected lazy val deletedFiles: Seq[String] = {
    val rels = previousIndexLogEntry.relations
    val originalFiles = rels.head.data.properties.content.fileInfos
    val currentFiles = rels.head.rootPaths
      .flatMap { p =>
        Content
          .fromDirectory(path = new Path(p), throwIfNotExists = true)
          .fileInfos
      }
      .map(f => f.name -> f)
      .toMap

    var delFiles = Seq[String]()
    originalFiles.foreach { f =>
      currentFiles.get(f.name) match {
        case Some(v) =>
          if (!f.equals(v)) {
            throw HyperspaceException(
              "Index refresh (to handle deleted source data) aborted. " +
                s"Existing source data file info is changed (file: ${f.name}).")
          }
        case None => delFiles :+= f.name
      }
    }

    delFiles
  }
}
