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

import org.apache.spark.SparkFunSuite
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import com.microsoft.hyperspace.SparkInvolvedSuite
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent}

class ActionTest extends SparkFunSuite with SparkInvolvedSuite with BeforeAndAfter {
  var mockLogManager: IndexLogManager = _
  var testObject: Action = _
  val testLogEntry: LogEntry = TestLogEntry(Constants.States.DOESNOTEXIST)

  before {
    mockLogManager = mock(classOf[IndexLogManager])
    when(mockLogManager.getLatestId()).thenReturn(None)
    when(mockLogManager.writeLog(anyInt(), any[LogEntry])).thenReturn(true)
    when(mockLogManager.deleteLatestStableLog()).thenReturn(true)

    testObject = new Action {
      override def logEntry: LogEntry = testLogEntry

      override def transientState: String = Constants.States.CREATING

      override def finalState: String = Constants.States.ACTIVE

      override def logManager: IndexLogManager = mockLogManager

      override def op(): Unit = {}

      override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent =
        new HyperspaceEvent {}
    }
  }

  test("verify run()") {
    testObject.run()
    testLogEntry.state = Constants.States.CREATING
    verify(mockLogManager).writeLog(0, testLogEntry)
    testLogEntry.state = Constants.States.ACTIVE
    verify(mockLogManager).writeLog(1, testLogEntry)
    verify(mockLogManager).deleteLatestStableLog()
    verify(mockLogManager).createLatestStableLog(1)
  }
}
