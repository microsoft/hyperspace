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
import com.microsoft.hyperspace.index.{Content, Directory, FileInfo, IndexConstants, IndexDataManager, IndexLogEntry, IndexLogManager}
import com.microsoft.hyperspace.index.IndexConstants.UNKNOWN_FILE_ID
import com.microsoft.hyperspace.index.covering.CoveringIndex

class VacuumOutdatedDataActionTest extends SparkFunSuite with SparkInvolvedSuite {
  private val mockLogManager: IndexLogManager = mock(classOf[IndexLogManager])
  private val mockDataManager: IndexDataManager = mock(classOf[IndexDataManager])
  private val mockIndexLogEntry: IndexLogEntry = mock(classOf[IndexLogEntry])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockLogManager.getLatestId()).thenReturn(None)
  }

  def versionDirectories(versions: Seq[Int]): Seq[String] = {
    versions.map(version =>
      s"file:/a/b/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=$version")
  }

  test("validate() passes if old index logs are found with ACTIVE state.") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(mockIndexLogEntry))
    when(mockIndexLogEntry.state).thenReturn(ACTIVE)
    val action = new VacuumOutdatedDataAction(mockLogManager, mockDataManager)
    // No exception thrown is considered a pass
    action.validate()
  }

  test("validate() fails if old index logs found with non-ACTIVE state") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(mockIndexLogEntry))
    when(mockIndexLogEntry.state).thenReturn(CREATING)
    val action = new VacuumOutdatedDataAction(mockLogManager, mockDataManager)
    val ex = intercept[HyperspaceException](action.validate())
    assert(
      ex.getMessage.contains(
        "VacuumOutdatedData is only supported in ACTIVE state. Current state is CREATING."))
  }

  test("op() calls which deletes nothing since every data is up-to-date") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(mockIndexLogEntry))
    when(mockDataManager.getAllVersionIds()).thenReturn(Seq(0, 1, 2))
    when(mockIndexLogEntry.indexDataDirectoryPaths())
      .thenReturn(versionDirectories(Seq(0, 1, 2)))

    val action = new VacuumOutdatedDataAction(mockLogManager, mockDataManager)
    action.op()
    verify(mockDataManager, new Times(0)).delete(-1)
    verify(mockDataManager, new Times(0)).delete(0)
    verify(mockDataManager, new Times(0)).delete(1)
    verify(mockDataManager, new Times(0)).delete(2)
    verify(mockDataManager, new Times(0)).delete(3)
  }

  test("op() calls delete for all outdated data") {
    when(mockLogManager.getLog(anyInt)).thenReturn(Some(mockIndexLogEntry))

    when(mockDataManager.getAllVersionIds()).thenReturn(Seq(0, 1, 2, 3))
    when(mockIndexLogEntry.indexDataDirectoryPaths()).thenReturn(versionDirectories(Seq(2, 3)))

    val action = new VacuumOutdatedDataAction(mockLogManager, mockDataManager)
    action.op()
    verify(mockDataManager).delete(0)
    verify(mockDataManager).delete(1)
    verify(mockDataManager, new Times(0)).delete(2)
    verify(mockDataManager, new Times(0)).delete(3)
    verify(mockDataManager, new Times(0)).delete(-1)
  }

  test("versionInfos gets correct version info.") {
    val versions = Seq(4, 5)

    val action = new VacuumOutdatedDataAction(mockLogManager, mockDataManager)
    val versionDirectory =
      versions.map(
        version =>
          Directory(
            s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=$version",
            files = Seq(FileInfo(s"index_$version", 0, 0, UNKNOWN_FILE_ID))))

    val content = Content(
      Directory(
        "file:/",
        subDirs = Seq(Directory(
          "a",
          files =
            Seq(FileInfo("f1", 0, 0, UNKNOWN_FILE_ID), FileInfo("f2", 0, 0, UNKNOWN_FILE_ID)),
          subDirs = Seq(
            Directory(
              "b",
              files =
                Seq(FileInfo("f3", 0, 0, UNKNOWN_FILE_ID), FileInfo("f4", 0, 0, UNKNOWN_FILE_ID)),
              subDirs = versionDirectory))))))

    val entry = IndexLogEntry.create(
      "indexName",
      CoveringIndex(Seq("col1"), Seq("col2", "col3"), null, 200, Map()),
      content,
      null,
      Map())

    val expected = versions.toSet
    val actual = action.dataVersionInfos(entry)
    assert(actual.equals(expected))
  }

}
