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

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshActionEvent}

/**
 * The Index refresh action is used to perform a full rebuild of the index.
 * Consequently, it ends up creating a new version of the index and involves
 * a full scan of the underlying source data.
 *
 * @param spark SparkSession.
 * @param logManager Index LogManager for index being refreshed.
 * @param dataManager Index DataManager for index being refreshed.
 */
class RefreshAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager)
    with Action {
  private lazy val (updatedIndex, indexData) = {
    updateFileIdTracker(spark, df)
    previousIndexLogEntry.derivedDataset.refreshFull(this, df)
  }

  override def logEntry: LogEntry =
    getIndexLogEntry(spark, df, previousIndexLogEntry.name, updatedIndex, indexDataPath, endId)

  final override def op(): Unit = updatedIndex.write(this, indexData)

  /**
   * Validate index is in active state for refreshing and there are some changes in
   * source data file(s).
   */
  final override def validate(): Unit = {
    super.validate()

    if (currentFiles.equals(previousIndexLogEntry.sourceFileInfoSet)) {
      throw NoChangesException("Refresh full aborted as no source data changed.")
    }
  }

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
