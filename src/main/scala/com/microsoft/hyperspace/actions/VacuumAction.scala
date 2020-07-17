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
import com.microsoft.hyperspace.actions.Constants.States.{DELETED, DOESNOTEXIST, VACUUMING}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, VacuumActionEvent}

class VacuumAction(
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends Action {
  final override lazy val logEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for vacuum operation")
    }
  }

  final override val transientState: String = VACUUMING

  final override val finalState: String = DOESNOTEXIST

  final override def validate(): Unit = {
    if (!logEntry.state.equalsIgnoreCase(DELETED)) {
      throw HyperspaceException(
        s"Vacuum is only supported in $DELETED state. " +
          s"Current state is ${logEntry.state}")
    }
  }

  final override def op(): Unit = {
    dataManager.getLatestVersionId().foreach { value =>
      (value to 0 by -1).foreach { id =>
        dataManager.delete(id)
      }
    }
  }

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    VacuumActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
