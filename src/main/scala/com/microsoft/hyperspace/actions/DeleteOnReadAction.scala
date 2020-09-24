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
import com.microsoft.hyperspace.telemetry.{AppInfo, DeleteOnReadActionEvent, HyperspaceEvent}

/**
 * Refresh index by updating list of excluded source data files and index signature
 * in index metadata.
 * Note this Refresh Action only fixes an index metadata w.r.t deleted source data files
 * and does not consider new source data files (if any).
 * If some original source data file(s) are removed between previous version of index and
 * now, this Action refreshes index as follows:
 * 1. Deleted source data files are identified.
 * 2. New index fingerprint is computed w.r.t latest source data files.
 * 3. IndexLogEntry is updated by modifying list of excluded source data files and
 * index fingerprint, computed in above steps.
 *
 * @param spark SparkSession.
 * @param logManager Index LogManager for index being refreshed.
 * @param dataManager Index DataManager for index being refreshed.
 */
class DeleteOnReadAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshDeleteActionBase(spark, logManager, dataManager)
    with Logging {

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    DeleteOnReadActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  final override def op(): Unit = {
    logInfo(
      "Refresh index is updating index metadata by adding " +
        s"${deletedFiles.length} deleted files to list of excluded source data files.")
  }

  /**
   * Compute new index fingerprint using latest source data files and create
   * new IndexLogEntry with updated list of excluded source data files and
   * new index fingerprint.
   *
   * @return updated IndexLogEntry.
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
                    excluded = excluded ++ (deletedFiles diff excluded)))))))))
  }
}
