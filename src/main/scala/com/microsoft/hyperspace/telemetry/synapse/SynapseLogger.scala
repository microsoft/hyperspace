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

package com.microsoft.hyperspace.telemetry.synapse

import com.microsoft.spark.telemetry.TelemetryEvent
import com.microsoft.spark.telemetry.mds.MdsEventLogger

import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.telemetry._

class SynapseLogger extends EventLogger with Serializable {
  private val logger: MdsEventLogger = new MdsEventLogger

  /**
   * Log an event to Synapse telemetry.
   *
   * @param event HyperspaceEvent to log in Synapse.
   */
  override def logEvent(event: HyperspaceEvent): Unit =
    logger.logEvent(new HyperspaceTelemetryEvent(event))

  /**
   * This class takes a HyperspaceEvent and wraps it in the TelemetryEvent interface that allows
   * it to be ingested into Synapse telemetry.
   *
   * @param event The underlying HyerspaceEvent to ingest.
   */
  private class HyperspaceTelemetryEvent(event: HyperspaceEvent) extends TelemetryEvent {
    private final val HYPERSPACE_TABLE = "HyperspaceEvents"

    private final val KEYS = Seq(
      "EventName",
      "SparkUser",
      "AppId",
      "AppName",
      "IndexNames",
      "IndexedColumnsIncludedColumns",
      "IndexPaths",
      "IndexSizes",
      "OriginalPlan",
      "ModifiedPlan",
      "Message")

    override def eventTableName: String = HYPERSPACE_TABLE

    override def dimensions: Map[String, Any] =
      KEYS.zip(getDimensionColumns).toMap

    private def getDimensionColumns: Seq[String] = {
      event match {
        case e: HyperspaceIndexCRUDEvent =>
          // CreateActionEvent can have a null index log entry.
          val (index, appInfo, message) = eventParams(e)
          val indexDims = if (index != null) {
            indexDimensions(Seq(index))
          } else {
            Seq("", "", "")
          }
          Seq(e.getClass.getSimpleName) ++
            appInfoDimensions(appInfo) ++
            indexDims ++
            Seq("0", "", "", message)
        case e: HyperspaceIndexUsageEvent =>
          Seq(e.getClass.getSimpleName) ++
            appInfoDimensions(e.appInfo) ++
            indexDimensions(e.indexes) ++
            Seq("0", "", "", e.message)
      }
    }

    private def eventParams(event: HyperspaceIndexCRUDEvent): (IndexLogEntry, AppInfo, String) = {
      event match {
        case e: CreateActionEvent => (e.index.orNull, e.appInfo, e.message)
        case e: DeleteActionEvent => (e.index, e.appInfo, e.message)
        case e: RestoreActionEvent => (e.index, e.appInfo, e.message)
        case e: VacuumActionEvent => (e.index, e.appInfo, e.message)
        case e: RefreshActionEvent => (e.index, e.appInfo, e.message)
        case e: CancelActionEvent => (e.index, e.appInfo, e.message)
        case e: RefreshIncrementalActionEvent => (e.index, e.appInfo, e.message)
        case e: RefreshQuickActionEvent => (e.index, e.appInfo, e.message)
        case e: OptimizeActionEvent => (e.index, e.appInfo, e.message)
      }
    }

    private def appInfoDimensions(appInfo: AppInfo): Seq[String] = {
      Seq(appInfo.sparkUser, appInfo.appId, appInfo.appName)
    }

    private def indexDimensions(entries: Seq[IndexLogEntry]): Seq[String] = {
      Seq(
        indexNameDimension(entries),
        indexConfigDimension(entries),
        indexLocationDimension(entries))
    }

    private def indexNameDimension(entries: Seq[IndexLogEntry]): String = {
      entries.map(_.config.indexName).mkString("; ")
    }

    private def indexConfigDimension(entries: Seq[IndexLogEntry]): String = {
      entries
        .map { index =>
          index.config.indexedColumns.mkString("[", ", ", "]") + "/" +
            index.config.includedColumns.mkString("[", ", ", "]")
        }
        .mkString("; ")
    }

    private def indexLocationDimension(entries: Seq[IndexLogEntry]): String = {
      entries.map(_.relations.head.rootPaths.mkString("; ")).mkString("; ")
    }
  }
}
