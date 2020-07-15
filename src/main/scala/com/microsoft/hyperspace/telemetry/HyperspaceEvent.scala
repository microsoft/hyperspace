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
 * Base class for all Hyperspace events.
 */
trait HyperspaceEvent

/**
 * General index CRUD event.
 *
 * @param sparkUser user of spark application.
 * @param appId spark application id.
 * @param appName spark application name.
 * @param index related indexes.
 * @param message message about event.
 */
class HyperspaceIndexCRUDEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String = StringUtils.EMPTY)
    extends HyperspaceEvent

case class CreateActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    originalPlan: String,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

case class DeleteActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

case class RestoreActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index)

case class VacuumActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

case class RefreshActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

case class CancelActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    index: IndexLogEntry,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, index, message)

case class HyperspaceIndexUsageEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    planBeforeRule: String,
    planAfterRule: String,
    message: String)
    extends HyperspaceEvent
