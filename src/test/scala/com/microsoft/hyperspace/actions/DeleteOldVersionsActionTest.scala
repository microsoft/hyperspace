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
import org.mockito.Mockito.{mock, verify, when}
import org.mockito.internal.verification.Times

import com.microsoft.hyperspace.{HyperspaceException, SparkInvolvedSuite}
import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._

class DeleteOldVersionsActionTest extends SparkFunSuite with SparkInvolvedSuite {
  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockLogManager.getLatestId()).thenReturn(None)
  }

  test("validate() passes if old index logs are found with ACTIVE state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(ACTIVE)))
    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() fails if old index logs found with non-ACTIVE state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(CREATING)))
    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(ex.getMessage.contains("DeleteOldVersions is only supported in ACTIVE state. Current state is CREATING."))
  }

  test("op() calls index datamanager.delete() for all data folders except the latest one") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(TestLogEntry(ACTIVE)))
    when(mockDataManager.getLatestVersionId()).thenReturn(Some(2))
    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    action.op()
    verify(mockDataManager).delete(0)
    verify(mockDataManager).delete(1)
    verify(mockDataManager, new Times(0)).delete(2)
    verify(mockDataManager, new Times(0)).delete(3)
    verify(mockDataManager, new Times(0)).delete(-1)
  }
}
