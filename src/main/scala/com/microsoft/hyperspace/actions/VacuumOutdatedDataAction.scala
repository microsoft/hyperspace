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

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, VACUUMINGOUTDATEDDATA}
import com.microsoft.hyperspace.index.{IndexDataManager, IndexLogEntry, IndexLogManager, LogEntry}
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, VacuumOutdatedDataActionEvent}

/**
 * Vacuum outdated data of indexes.
 *
 * Algorithm:
 *  - Delete every version except the latest versions.
 */
class VacuumOutdatedDataAction(
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends Action {
  override def logEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for vacuum outdated data operation")
    }
  }

  override def transientState: String = VACUUMINGOUTDATEDDATA

  override def finalState: String = ACTIVE

  override def validate(): Unit = {
    if (!logEntry.state.equalsIgnoreCase(ACTIVE)) {
      throw HyperspaceException(
        s"VacuumOutdatedData is only supported in $ACTIVE state. " +
          s"Current state is ${logEntry.state}.")
    }
  }

  final override def op(): Unit = {
    // Get versions used in the latest log entry.
    val indexVersionsInUse: Set[Int] = logEntry match {
      case indexLogEntry: IndexLogEntry =>
        indexLogEntry.versionInfos
      case other =>
        throw HyperspaceException(
          s"VacuumOutdatedData is not defined with log entry class ${other.getClass.getName}")
    }

    // delete version not used
    dataManager.getAllVersionIds().foreach { id =>
      if (!indexVersionsInUse.contains(id)) {
        dataManager.delete(id)
      }
    }
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    VacuumOutdatedDataActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
