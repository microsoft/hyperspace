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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{mock, when}
import org.scalatest.FunSuite

import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.{HyperspaceException, SparkInvolvedSuite}

class RestoreActionTest extends FunSuite with SparkInvolvedSuite {
  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockLogManager.getLatestId()).thenReturn(None)
  }

  test("validate() passes if old index logs are found with DELETED state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(DELETED)))
    val action = new RestoreAction(mockLogManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() fails if old index logs found with non-DELETED state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(CREATING)))
    val action = new RestoreAction(mockLogManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(ex.getMessage.contains("Restore is only supported in DELETED state"))
  }
}
