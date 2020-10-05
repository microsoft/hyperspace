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
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{DataType, StructType}

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
 * @param spark SparkSession
 * @param logManager Index LogManager for index being refreshed
 * @param dataManager Index DataManager for index being refreshed
 */
class RefreshIncrementalAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {
  final override def op(): Unit = {
    // TODO: The current implementation picks the number of buckets from session config.
    //   This should be user-configurable to allow maintain the existing bucket numbers
    //   in the index log entry.
    write(spark, indexableDf, indexConfig)
  }

  private lazy val indexableDf = {
    val relation = previousIndexLogEntry.relations.head

    // TODO: improve this to take last modified time of files into account.
    val indexedFiles = relation.data.properties.content.files.map(_.toString)

    val allFiles = df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.allFiles().map(_.getPath.toString)
    }.flatten

    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]

    // Create a df with only diff files from original list of files.
    spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(allFiles.diff(indexedFiles): _*)
  }

  /**
   * Create a logEntry with all data files, and index content merged with previous index content.
   * This contains ALL data files (files which were indexed previously, and files which are being
   * indexed in this operation). It also contains ALL index files (index files for previously
   * indexed data as well as newly created files).
   *
   * @return Merged index log entry.
   */
  override def logEntry: LogEntry = {
    // Log entry with complete data and newly index files.
    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)

    // Merge new index files with old index files.
    val mergedContent = Content(
      previousIndexLogEntry.content.root.merge(entry.content.root)
    )

    // New entry.
    entry.copy(content = mergedContent)
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshAppendActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
