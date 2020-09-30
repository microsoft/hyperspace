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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshLogEntryActionEvent}

/**
 * Refresh index by updating list of excluded and appended source data files and
 * index signature in index metadata.
 * If some original source data file(s) are removed or appended between previous
 * version of index and now, this Action refreshes index as follows:
 * 1. Deleted and appended source data files are identified.
 * 2. New index fingerprint is computed w.r.t latest source data files. This captures
 *    both deleted source data files and appended ones.
 * 3. IndexLogEntry is updated by modifying list of excluded and appended source data
 *    files and index fingerprint, computed in above steps.
 *
 * @param spark SparkSession.
 * @param logManager Index LogManager for index being refreshed.
 * @param dataManager Index DataManager for index being refreshed.
 */
class RefreshLogEntryAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshDeleteActionBase(spark, logManager, dataManager)
    with Logging {

  private lazy val newExcludedFiles: Seq[String] =
    sourceFilesDiff._1 diff previousIndexLogEntry.excludedFiles

  private lazy val newAppendedFiles: Seq[String] =
    sourceFilesDiff._2 diff previousIndexLogEntry.appendedFiles

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshLogEntryActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  override def validate(): Unit = {
    super.validate()
    if (newExcludedFiles.isEmpty && newAppendedFiles.isEmpty) {
      throw HyperspaceException(
        "Refresh aborted as no new deleted or appended source data file found.")
    }
  }

  final override def op(): Unit = {
    logInfo("Refresh index is updating index metadata by adding " +
      s"${newExcludedFiles.length} new deleted files to list of excluded source data files and " +
      s"${newAppendedFiles.length} new appended files to list of appended source data files.")
  }

  /**
   * Compute new index fingerprint using latest source data files and create
   * new IndexLogEntry with updated list of excluded and appended source data
   * files and new index fingerprint.
   *
   * @return Updated IndexLogEntry.
   */
  final override def logEntry: LogEntry = {
    // Compute index fingerprint using current source data file.
    val signatureProvider = LogicalPlanSignatureProvider.create()
    val newSignature = signatureProvider.signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        LogicalPlanFingerprint(
          LogicalPlanFingerprint.Properties(Seq(Signature(signatureProvider.name, s))))

      case None => throw HyperspaceException("Invalid source plan found during index refresh.")
    }

    // Grab nested structures from previous IndexLogEntry.
    val source = previousIndexLogEntry.source
    val plan = source.plan
    val planProps = plan.properties
    val relation = planProps.relations.head
    val data = relation.data
    val dataProps = data.properties
    val excluded = dataProps.excluded
    val appended = dataProps.appended

    // Create a new IndexLogEntry by updating excluded files and fingerprint.
    previousIndexLogEntry.copy(
      source = source.copy(
        plan = plan.copy(
          properties = planProps.copy(
            fingerprint = newSignature,
            relations = Seq(
              relation.copy(
                data = data.copy(
                  properties = dataProps.copy(
                    excluded = excluded ++ newExcludedFiles,
                    appended = appended ++ newAppendedFiles))))))))
  }
}
