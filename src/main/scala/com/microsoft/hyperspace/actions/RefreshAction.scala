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
 * Index refresh action. This action is used when refresh does a full rebuild of index.
 * This action does not use any existing index file and a newer version of index is built via
 * a full scan and shuffle of latest source data files w.r.t index configs.
 *
 * @param spark SparkSession
 * @param logManager Index LogManager for index being refreshed
 * @param dataManager Index DataManager for index being refreshed
 */
class RefreshAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {

  final override def op(): Unit = {
    // TODO: The current implementation picks the number of buckets from session config.
    //   This should be user-configurable to allow maintain the existing bucket numbers
    //   in the index log entry.
    write(spark, df, indexConfig)
  }

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
