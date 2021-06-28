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

import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshIncrementalActionEvent}

/**
 * Action to refresh indexes with newly appended files and deleted files in an incremental way.
 *
 * For appended files, newly arrived data in the original source dataset (more specifically under
 * rootPaths), will be handled as follows:
 * - Identify newly added data files.
 * - Create new index version on these files.
 * - Update metadata to reflect the latest snapshot of index. This snapshot includes all the old
 *   and the newly created index files. The source content points to the latest data files.
 *
 * For deleted files, some original source data file(s) are removed between previous version of
 * index and now, will be handled as follows:
 * - Identify deleted source data files.
 * - Index records' lineage is leveraged to remove any index entry coming
 *    from those deleted source data files.
 *
 * @param spark SparkSession.
 * @param logManager Index LogManager for index being refreshed.
 * @param dataManager Index DataManager for index being refreshed.
 */
class RefreshIncrementalAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager)
    with Action {

  final override def op(): Unit = {
    logInfo(
      "Refresh index is updating index by removing index entries " +
        s"corresponding to ${deletedFiles.length} deleted source data files and " +
        s"indexing ${appendedFiles.length} appended source data files.")

    val appendedSourceData = if (appendedFiles.nonEmpty) {
      val internalFileFormatName = Hyperspace
        .getContext(spark)
        .sourceProviderManager
        .getRelationMetadata(previousIndexLogEntry.relations.head)
        .internalFileFormatName()

      // Create a df with only appended files from original list of files.
      val dfWithAppendedFiles = spark.read
        .schema(df.schema)
        .format(internalFileFormatName)
        .options(previousIndexLogEntry.relations.head.options)
        .load(appendedFiles.map(_.name): _*)
      Some(dfWithAppendedFiles)
    } else {
      None
    }
    val (updatedIndex, updateMode) =
      previousIndexLogEntry.derivedDataset.refreshIncremental(
        this,
        appendedSourceData,
        deletedFiles,
        previousIndexLogEntry.content)
    updatedIndexOpt = Some(updatedIndex)
    updateModeOpt = Some(updateMode)
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

    // To handle deleted files, lineage column is required for the index.
    if (deletedFiles.nonEmpty && !previousIndexLogEntry.derivedDataset.canHandleDeletedFiles) {
      throw HyperspaceException(
        "Index refresh (to handle deleted source data) is " +
          "only supported on an index with lineage.")
    }
  }

  private var updatedIndexOpt: Option[Index] = None
  private var updateModeOpt: Option[Index.UpdateMode] = None

  /**
   * Create a log entry with all source data files, and all required index content. This contains
   * ALL source data files (files which were indexed previously, and files which are being indexed
   * in this operation). It also contains ALL index files (indexed data for previously indexed
   * files as well as newly updated index files).
   *
   * @return Refreshed index log entry.
   */
  override def logEntry: LogEntry = {
    val index = updatedIndexOpt.getOrElse(previousIndexLogEntry.derivedDataset)
    val entry =
      getIndexLogEntry(spark, df, previousIndexLogEntry.name, index, indexDataPath, endId)

    if (updateModeOpt.contains(Index.UpdateMode.Merge)) {
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
