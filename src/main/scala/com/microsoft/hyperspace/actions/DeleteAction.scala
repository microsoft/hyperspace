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
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, DELETED, DELETING}
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogManager, LogEntry}
import com.microsoft.hyperspace.telemetry.{AppInfo, DeleteActionEvent, HyperspaceEvent}

class DeleteAction(final override protected val logManager: IndexLogManager) extends Action {
  final override lazy val logEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for delete operation")
    }
  }

  final override val transientState: String = DELETING

  final override val finalState: String = DELETED

  final override def validate(): Unit = {
    if (!logEntry.state.equalsIgnoreCase(ACTIVE)) {
      throw HyperspaceException(
        s"Delete is only supported in $ACTIVE state. " +
          s"Current state is ${logEntry.state}")
    }
  }

  final override def op(): Unit = { /* Do nothing */ }

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    DeleteActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
