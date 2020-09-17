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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshDeleteActionEvent}

/**
 * Refresh index by removing index entries from any deleted source data file.
 * Note this Refresh Action only fixes an index w.r.t deleted source data files
 * and does not consider new source data files (if any).
 * If some original source data file(s) are removed between previous version of index and now,
 * this Refresh Action updates the index as follows:
 * 1. Deleted source data files are identified;
 * 2. Index records' lineage is leveraged to remove any index entry coming
 *    from those deleted source data files.
 *
 * @param spark SparkSession
 * @param logManager Index LogManager for index being refreshed
 * @param dataManager Index DataManager for index being refreshed
 */
class RefreshDeleteAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager)
    with Logging {

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshDeleteActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  /**
   * For an index with lineage, find all the source data files which have been deleted,
   * and use index records' lineage to mark and remove index entries which belong to
   * deleted source data files as those entries are no longer valid.
   */
  final override def op(): Unit = {
    if (!previousIndexLogEntry.hasLineageColumn(spark)) {
      throw HyperspaceException(
        s"Index refresh (to handle deleted source data) is" +
          " only supported on an index with lineage.")
    }

    val deletedFiles = getDeletedFiles
    if (deletedFiles.isEmpty) {
      throw HyperspaceException("Refresh aborted as no deleted source data file found.")
    }

    logInfo(
      "Refresh index is updating index by removing index entries" +
        s" corresponding to ${deletedFiles.length} deleted source data files.")

    val refreshDF =
      spark.read
        .parquet(previousIndexLogEntry.content.files.map(_.toString): _*)
        .filter(!col(s"${IndexConstants.DATA_FILE_NAME_COLUMN}").isin(deletedFiles: _*))

    refreshDF.write.saveWithBuckets(
      refreshDF,
      indexDataPath.toString,
      previousIndexLogEntry.numBuckets,
      indexConfig.indexedColumns)
  }

  /**
   * Compare list of source data files from previous IndexLogEntry to list of
   * current source data files and identify deleted source data files.
   *
   * @return list of full paths of deleted source data files.
   */
  private def getDeletedFiles: Seq[String] = {
    val rels = previousIndexLogEntry.relations
    val originalFiles = rels.head.data.properties.content.files.map(_.toString)
    val currentFiles = rels.head.rootPaths.flatMap { p =>
      Content
        .fromDirectory(path = new Path(p), throwIfNotExists = true)
        .files
        .map(_.toString)
    }

    originalFiles diff currentFiles
  }
}
