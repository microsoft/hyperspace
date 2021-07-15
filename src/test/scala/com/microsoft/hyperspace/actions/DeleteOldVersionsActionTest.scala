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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{mock, verify, when}
import org.mockito.internal.verification.Times

import com.microsoft.hyperspace.{HyperspaceException, SparkInvolvedSuite}
import com.microsoft.hyperspace.actions.Constants.States._
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.covering.CoveringIndex

class DeleteOldVersionsActionTest extends SparkFunSuite with SparkInvolvedSuite {
  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])
  private val mockContent: Content = mock(classOf[Content])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockLogManager.getLatestId()).thenReturn(None)
  }

  def testEntry(state: String): IndexLogEntry = {
    val entry = IndexLogEntry(
      "index1",
      CoveringIndex(
        Seq("clicks"),
        Seq(),
        StructType(StructField("clicks", IntegerType) :: Nil),
        10,
        Map()),
      mockContent,
      Source(null),
      Map())
    entry.state = state
    entry
  }

  test("validate() passes if old index logs are found with ACTIVE state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testEntry(ACTIVE)))
    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() fails if old index logs found with non-ACTIVE state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testEntry(CREATING)))
    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(
      ex.getMessage.contains(
        "DeleteOldVersions is only supported in ACTIVE state. Current state is CREATING."))
  }

  test("op() calls index datamanager.delete() which deletes nothing") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testEntry(ACTIVE)))

    when(mockDataManager.getAllVersionIds()).thenReturn(Set(0, 1, 2))
    when(mockContent.versionInfos).thenReturn(Set(0, 1, 2))

    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    action.op()
    verify(mockDataManager, new Times(0)).delete(-1)
    verify(mockDataManager, new Times(0)).delete(0)
    verify(mockDataManager, new Times(0)).delete(1)
    verify(mockDataManager, new Times(0)).delete(2)
    verify(mockDataManager, new Times(0)).delete(3)
  }

  test("op() calls index datamanager.delete() for all data folders except the used ones") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(testEntry(ACTIVE)))

    when(mockDataManager.getAllVersionIds()).thenReturn(Set(0, 1, 2, 3))
    when(mockContent.versionInfos).thenReturn(Set(2, 3))

    val action = new DeleteOldVersionsAction(mockLogManager, mockDataManager)
    action.op()
    verify(mockDataManager).delete(0)
    verify(mockDataManager).delete(1)
    verify(mockDataManager, new Times(0)).delete(2)
    verify(mockDataManager, new Times(0)).delete(3)
    verify(mockDataManager, new Times(0)).delete(-1)
  }
}
