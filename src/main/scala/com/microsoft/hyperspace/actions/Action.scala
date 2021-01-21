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

import com.microsoft.hyperspace.{ActiveSparkSession, HyperspaceException}
import com.microsoft.hyperspace.index.{IndexLogManager, LogEntry}
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, HyperspaceEventLogging}

/**
 * This is a generic Index-Modifying Action interface. It provides APIs to begin and commit
 * operations which logically lock an index from further operations.
 *
 * TODO: Action classes can be revisited to make them more generic to support more types of
 *   metadata:
 *     1. Any metadata dependent logic should be passed in as functions instead.
 *     2. IndexLogEntry specific code should be removed.
 */
trait Action extends HyperspaceEventLogging with Logging with ActiveSparkSession {
  protected val baseId: Int = logManager.getLatestId().getOrElse(-1)
  final protected def endId: Int = baseId + 2

  def logEntry: LogEntry

  def transientState: String

  def finalState: String

  protected def logManager: IndexLogManager

  // TODO: Revisit if access modifier can be changed to protected.
  def validate(): Unit = {}

  private def begin(): Unit = {
    val newId = baseId + 1
    val entry = logEntry
    entry.state = transientState
    entry.id = newId
    saveEntry(newId, entry)
  }

  // TODO: Revisit if access modifier can be changed to protected.
  def op(): Unit

  private def end(): Unit = {
    val newId = endId
    val entry = logEntry
    entry.state = finalState
    entry.id = newId

    if (!logManager.deleteLatestStableLog()) {
      throw HyperspaceException("Could not delete latest stable log")
    }

    saveEntry(newId, entry)

    if (!logManager.createLatestStableLog(newId)) {
      logWarning("Unable to recreate latest stable log")
    }
  }

  private def saveEntry(id: Int, entry: LogEntry): Unit = {
    entry.timestamp = System.currentTimeMillis()
    if (!logManager.writeLog(id, entry)) {
      throw HyperspaceException("Could not acquire proper state")
    }
  }

  def run(): Unit = {
    val appInfo =
      AppInfo(sparkContext.sparkUser, sparkContext.applicationId, sparkContext.appName)
    try {
      logEvent(event(appInfo, "Operation started."))
      validate()

      begin()

      op()

      end()
      logEvent(event(appInfo, message = "Operation succeeded."))
    } catch {
      case e: NoChangesException =>
        logEvent(event(appInfo, message = s"No-op operation recorded: ${e.getMessage}"))
        logWarning(e.msg)
      case e: Exception =>
        logEvent(event(appInfo, message = s"Operation failed: ${e.getMessage}"))
        throw e
    }
  }

  protected def event(appInfo: AppInfo, message: String): HyperspaceEvent
}
