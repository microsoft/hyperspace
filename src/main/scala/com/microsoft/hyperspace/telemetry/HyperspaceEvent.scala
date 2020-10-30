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

package com.microsoft.hyperspace.telemetry

import com.microsoft.hyperspace.index.{IndexConfig, IndexLogEntry}

/**
 * Class for common app info.
 *
 * @param sparkUser Spark user.
 * @param appId Spark Application Id.
 * @param appName Spark App Name.
 */
case class AppInfo(sparkUser: String, appId: String, appName: String)

/**
 * Trait for all Hyperspace events.
 */
trait HyperspaceEvent

/**
 * General index CRUD event.
 */
trait HyperspaceIndexCRUDEvent extends HyperspaceEvent

/**
 * Index creation event. Emitted on index creation.
 *
 * @param appInfo AppInfo for spark application.
 * @param indexConfig Index config used in index creation.
 * @param index Optional index. It can be None if create index fails with invalid config.
 * @param originalPlan Original plan which is getting indexed.
 * @param message Message about event.
 */
case class CreateActionEvent(
    appInfo: AppInfo,
    indexConfig: IndexConfig,
    index: Option[IndexLogEntry],
    originalPlan: String,
    message: String)
    extends HyperspaceIndexCRUDEvent

/**
 * Index deletion event. Emitted when delete is called on an index.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class DeleteActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
    extends HyperspaceIndexCRUDEvent

/**
 * Index restore event. Emitted when restore is called on an index.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class RestoreActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
    extends HyperspaceIndexCRUDEvent

/**
 * Index Vacuum Event. Emitted when vacuum is called on an index.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class VacuumActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
    extends HyperspaceIndexCRUDEvent

/**
 * Index Refresh Event. Emitted when refresh is called on an index.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class RefreshActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
    extends HyperspaceIndexCRUDEvent

/**
 * Index cancel event. This event is emitted when the User cancels an ongoing or failed CRUD
 * operation.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class CancelActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
    extends HyperspaceIndexCRUDEvent

/**
 * Index Refresh Event for incremental mode. Emitted when refresh is called on an index
 * with "incremental" mode to handle appended/deleted source data files.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class RefreshIncrementalActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
  extends HyperspaceIndexCRUDEvent

/**
 * Index Refresh Event for quick mode. Emitted when refresh is called on an index
 * with "quick" mode to update index metadata only for appended/deleted source data files.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class RefreshQuickActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
  extends HyperspaceIndexCRUDEvent

/**
 * Index Optimize Event for index files.
 *
 * @param appInfo AppInfo for spark application.
 * @param index Related index.
 * @param message Message about event.
 */
case class OptimizeActionEvent(appInfo: AppInfo, index: IndexLogEntry, message: String)
  extends HyperspaceIndexCRUDEvent

/**
 * Index usage event. This event is emitted when an index is picked instead of original data
 * source by one of the hyperspace rules.
 *
 * @param appInfo AppInfo for spark application.
 * @param indexes List of selected indexes for this plan.
 * @param planBeforeRule Original plan before application of indexes.
 * @param planAfterRule Plan after using indexes.
 * @param message Message about event.
 */
case class HyperspaceIndexUsageEvent(
    appInfo: AppInfo,
    indexes: Seq[IndexLogEntry],
    planBeforeRule: String,
    planAfterRule: String,
    message: String)
    extends HyperspaceEvent
