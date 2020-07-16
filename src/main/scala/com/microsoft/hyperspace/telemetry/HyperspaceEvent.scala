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

import org.apache.commons.lang.StringUtils

import com.microsoft.hyperspace.index.IndexLogEntry

/**
 * Trait for all Hyperspace events.
 */
trait HyperspaceEvent

/**
 * General index CRUD event.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param message Message about event.
 */
class HyperspaceIndexCRUDEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String = StringUtils.EMPTY)
    extends HyperspaceEvent

/**
 * Index creation event. Emitted on index creation.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param originalPlan Original plan which is getting indexed.
 * @param message Message about event.
 */
case class CreateActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    originalPlan: String,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

/**
 * Index deletion event. Emitted when delete is called on an index.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param message Message about event.
 */
case class DeleteActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

/**
 * Index restore event. Emitted when restore is called on an index.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param message Message about event.
 */
case class RestoreActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index)

/**
 * Index Vacuum Event. Emitted when vacuum is called on an index.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param message Message about event.
 */
case class VacuumActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

/**
 * Index Refresh Event. Emitted when refresh is called on an index.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param message Message about event.
 */
case class RefreshActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

/**
 * Index cancel event. This event is emitted when the User cancels an ongoing or failed CRUD
 * operation.
 *
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param index Related indexes.
 * @param message Message about event.
 */
case class CancelActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

/**
* Index usage event. This event is emitted when an index is picked instead of original data
 * source by one of the hyperspace rules.
 * @param sparkUser User of spark application.
 * @param appId Spark application id.
 * @param appName Spark application name.
 * @param indexes List of selected indexes for this plan.
 * @param planBeforeRule Original plan before application of indexes.
 * @param planAfterRule Plan after using indexes.
 * @param message Message about event.
 */
case class HyperspaceIndexUsageEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    planBeforeRule: String,
    planAfterRule: String,
    message: String)
    extends HyperspaceEvent
