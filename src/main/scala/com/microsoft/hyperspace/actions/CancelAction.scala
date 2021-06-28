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
import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogManager, LogEntry}
import com.microsoft.hyperspace.telemetry.{AppInfo, CancelActionEvent, HyperspaceEvent}

/**
 * Cancelling an action. This action is used if index maintenance operations fail and leave the
 * index in a hanging intermediate state. E.g. If refresh action fails, the index is in
 * [[REFRESHING]] state, preventing future index operations. Cancelling brings the index
 * back to the last known stable state.
 *
 * Algorithm:
 *  - Find the last stable active state log entry
 *  - save the next log entry with the contents of the last active state log entry
 *  - TODO: (optionally cleanup any partial files created during previous jobs)
 */
class CancelAction(final override protected val logManager: IndexLogManager) extends Action {
  final override lazy val logEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for cancel operation")
    }
  }

  final override val transientState: String = CANCELLING

  final override lazy val finalState: String = {
    logManager.getLatestStableLog() match {
      case None => DOESNOTEXIST
      case Some(entry) => entry.state
    }
  }

  final override def validate(): Unit = {
    if (Constants.STABLE_STATES.contains(logEntry.state)) {
      throw HyperspaceException(
        s"Cancel() is not supported in ${Constants.STABLE_STATES} states. " +
          s"Current state is ${logEntry.state}")
    }
  }

  /*
   * TODO: Can be improved to clean up partially created files in previous incomplete operations
   */
  final override def op(): Unit = {}

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    CancelActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
