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

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshQuickActionEvent}

/**
 * Action to refresh index metadata only with newly appended files and deleted files.
 *
 * @param spark SparkSession.
 * @param logManager Index LogManager for index being refreshed.
 * @param dataManager Index DataManager for index being refreshed.
 */
class RefreshQuickAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {
  final override def op(): Unit = {
    logInfo(
      s"Refresh index is updating metadata only with ${deletedFiles.size} of deleted " +
        s"files and ${appendedFiles.size} of appended files.")
  }

  /**
   * Validate index is in active state for refreshing and there are some changes
   * in source data file(s).
   */
  final override def validate(): Unit = {
    super.validate()

    if (appendedFiles.isEmpty && deletedFiles.isEmpty) {
      throw NoChangesException("Refresh quick aborted as no source data change found.")
    }

    // To handle deleted files, lineage column is required for the index.
    if (deletedFiles.nonEmpty && !previousIndexLogEntry.hasLineageColumn) {
      throw HyperspaceException(
        "Index refresh to handle deleted source data is only supported on an index with lineage.")
    }
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshQuickActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  /**
   * Create a log entry with metadata update for appended files and deleted files.
   *
   * @return Refreshed index log entry.
   */
  override def logEntry: LogEntry = {
    val signatureProvider = LogicalPlanSignatureProvider.create()
    val latestFingerprint = LogicalPlanFingerprint(
      LogicalPlanFingerprint.Properties(
        Seq(
          Signature(
            signatureProvider.name,
            signatureProvider.signature(df.queryExecution.optimizedPlan).get))))
    previousIndexLogEntry.copyWithUpdate(latestFingerprint, appendedFiles, deletedFiles)
  }
}
