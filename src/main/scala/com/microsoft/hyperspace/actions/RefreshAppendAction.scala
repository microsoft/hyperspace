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

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshAppendActionEvent}

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
class RefreshAppendAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {

  private lazy val fileIdsMap = getFileIdsMap(dfWithAppendedFiles)

  final override def op(): Unit = {
    // TODO: The current implementation picks the number of buckets and
    //  "spark.hyperspace.index.lineage.enabled" from session config. This should be
    //  user-configurable to allow maintain the existing bucket numbers in the index log entry.
    //  https://github.com/microsoft/hyperspace/issues/196.
    write(spark, dfWithAppendedFiles, indexConfig, fileIdsMap._1)
  }

  /**
   * Validate index is in active state for refreshing and there are some appended
   * source data file(s).
   */
  final override def validate(): Unit = {
    super.validate()

    if (appendedFiles.isEmpty) {
      throw NoChangesException("Refresh append aborted as no appended source data files found.")
    }
  }

  private lazy val dfWithAppendedFiles = {
    val relation = previousIndexLogEntry.relations.head
    // Create a df with only diff files from original list of files.
    spark.read
      .schema(df.schema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(appendedFiles.map(_.name): _*)
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
    val entry =
      getIndexLogEntry(spark, df, indexConfig, indexDataPath, fileIdsMap._1, fileIdsMap._2)

    // Merge new index files with old index files.
    val mergedContent = Content(previousIndexLogEntry.content.root.merge(entry.content.root))

    // New entry.
    entry.copy(content = mergedContent).withAppendedAndDeletedFiles(Seq(), deletedFiles)
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshAppendActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  /**
   * Generate a mapping for source data files by assigning a unique file id
   * to each source data file. Once assigned, this file id does not change
   * for a given file and is used to refer to that file.
   * We extract last file id from previous version of index and start
   * assigning file ids to new source data files according to that
   * to make sure there is no gap or duplicate in file ids.
   *
   * @param df DataFrame to index.
   * @return mapping of source data file paths to their file ids, and the
   *         last assigned file id.
   */
  final override def getFileIdsMap(df: DataFrame): (Map[String, Long], Long) = {
    var lastFileId = previousIndexLogEntry.lastFileId
    val fileIdsMap = mutable.Map() ++ previousIndexLogEntry.fileIdsMap
    appendedFiles.foreach { f =>
      lastFileId += 1
      fileIdsMap.put(f.name, lastFileId)
    }
    (fileIdsMap.toMap, lastFileId)
  }
}
