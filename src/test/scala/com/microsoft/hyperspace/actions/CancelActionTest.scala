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
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{mock, when}

import com.microsoft.hyperspace.SparkInvolvedSuite
import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._

class CancelActionTest extends SparkFunSuite with SparkInvolvedSuite {
  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockLogManager.getLatestId()).thenReturn(None)
  }

  test("Cancel leads to ACTIVE from ACTIVE state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(ACTIVE)))
    when(mockLogManager.getLatestStableLog()).thenReturn(Some(TestLogEntry(ACTIVE)))

    val action = new CancelAction(mockLogManager)
    assert(action.finalState === ACTIVE)
  }

  test("Cancel leads to last stable state from transient state if stable state exists") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(REFRESHING)))
    when(mockLogManager.getLatestStableLog()).thenReturn(Some(TestLogEntry(ACTIVE)))

    val action = new CancelAction(mockLogManager)
    assert(action.finalState === ACTIVE)
  }

  test("Cancel leads to DoesNotExist state from VACUUMING") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(VACUUMING)))
    when(mockLogManager.getLatestStableLog()).thenReturn(None)

    val action = new CancelAction(mockLogManager)
    assert(action.finalState === DOESNOTEXIST)
  }

  test("Cancel leads to DoesNotExist from transient state if no stable state exists") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(REFRESHING)))
    when(mockLogManager.getLatestStableLog()).thenReturn(None)

    val action = new CancelAction(mockLogManager)
    assert(action.finalState === DOESNOTEXIST)
  }
}
