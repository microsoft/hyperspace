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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshIncrementalActionEvent}

/**
 * Action to create indexes on newly arrived data. If the user appends new data to existing,
 * pre-indexed data, they can use refresh api to generate indexes only on the additional data.
 *
 * Algorithm Outline:
 * - Identify newly added data files.
 * - Create new index version on these files.
 * - Update metadata to reflect the latest snapshot of index. This snapshot includes all the old
 *   and the newly created index files. The source content points to the latest data files.
 *
 * @param spark SparkSession.
 * @param logManager Index LogManager for index being refreshed.
 * @param dataManager Index DataManager for index being refreshed.
 */
class RefreshIncrementalAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {
  final override def op(): Unit = {
    logInfo(
      "Refresh index is updating index by removing index entries " +
        s"corresponding to ${deletedFiles.length} deleted source data files.")

    if (appendedFiles.nonEmpty) {
      val dfWithAppendedFiles = {
        val relation = previousIndexLogEntry.relations.head
        // Create a df with only diff files from original list of files.
        spark.read
          .schema(df.schema)
          .format(relation.fileFormat)
          .options(relation.options)
          .load(appendedFiles.map(_.name): _*)
      }
      write(spark, dfWithAppendedFiles, indexConfig)
    }

    if (deletedFiles.nonEmpty) {
      val refreshDF =
        spark.read
          .parquet(previousIndexLogEntry.content.files.map(_.toString): _*)
          .filter(
            !col(s"${IndexConstants.DATA_FILE_NAME_COLUMN}").isin(deletedFiles.map(_.name): _*))

      refreshDF.write.saveWithBuckets(
        refreshDF,
        indexDataPath.toString,
        previousIndexLogEntry.numBuckets,
        indexConfig.indexedColumns,
        SaveMode.Append)
    }
  }

  /**
   * Validate index is in active state for refreshing and there are some changes
   * in source data file(s).
   */
  final override def validate(): Unit = {
    super.validate()

    if (appendedFiles.isEmpty && deletedFiles.isEmpty) {
      throw NoChangesException("Refresh incremental aborted as no source data change found.")
    }

    if (deletedFiles.nonEmpty && !previousIndexLogEntry.hasLineageColumn(spark)) {
      throw HyperspaceException(
        "Index refresh (to handle deleted source data) is " +
          "only supported on an index with lineage.")
    }
  }


  /**
   * Create a log entry with all data files, and index content merged with previous index content.
   * This contains ALL data files (files which were indexed previously, and files which are being
   * indexed in this operation). It also contains ALL index files (index files for previously
   * indexed data as well as newly created files).
   *
   * @return Merged index log entry.
   */
  override def logEntry: LogEntry = {
    // Log entry with complete data and newly index files.
    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)

    if (deletedFiles.isEmpty) {
      // Merge new index files with old index files.
      val mergedContent = Content(previousIndexLogEntry.content.root.merge(entry.content.root))
      entry.copy(content = mergedContent)
    } else {
      // New entry.
      entry
    }
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshIncrementalActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
