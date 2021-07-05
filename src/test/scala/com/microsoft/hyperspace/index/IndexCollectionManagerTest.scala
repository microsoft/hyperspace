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

package com.microsoft.hyperspace.index

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.IndexConstants.{REFRESH_MODE_FULL, REFRESH_MODE_INCREMENTAL}
import com.microsoft.hyperspace.index.covering.CoveringIndex

class IndexCollectionManagerTest extends HyperspaceSuite {
  private val testLogManagerFactory: IndexLogManagerFactory = new IndexLogManagerFactory {
    override def create(indexPath: Path, hadoopConfiguration: Configuration): IndexLogManager =
      new IndexLogManager {
        override def getLog(id: Int): Option[LogEntry] = Some(testLogEntry)
        override def getLatestId(): Option[Int] = Some(0)
        override def getLatestStableLog(): Option[LogEntry] = throw new NotImplementedError
        override def createLatestStableLog(id: Int): Boolean = throw new NotImplementedError
        override def deleteLatestStableLog(): Boolean = throw new NotImplementedError
        override def writeLog(id: Int, log: LogEntry): Boolean = throw new NotImplementedError
        override def getIndexVersions(states: Seq[String]): Seq[Int] = Seq(0)

        private val testLogEntry: IndexLogEntry = {
          val sourcePlanProperties = SparkPlan.Properties(
            Seq(),
            null,
            null,
            LogicalPlanFingerprint(LogicalPlanFingerprint.Properties(Seq(Signature("", "")))))

          val entry = IndexLogEntry(
            indexPath.toString,
            CoveringIndex(Seq("RGUID"), Seq("Date"), new StructType(), 10, Map()),
            Content(Directory(s"$indexPath/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")),
            Source(SparkPlan(sourcePlanProperties)),
            Map())
          entry.state = Constants.States.ACTIVE
          entry
        }
      }
  }

  private val mockFileSystem: FileSystem = mock(classOf[FileSystem])
  private val mockFileSystemFactory: FileSystemFactory = mock(classOf[FileSystemFactory])
  private var indexCollectionManager: IndexCollectionManager = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockFileSystemFactory.create(any[Path], any[Configuration])).thenReturn(mockFileSystem)

    indexCollectionManager = new IndexCollectionManager(
      spark,
      testLogManagerFactory,
      IndexDataManagerFactoryImpl,
      mockFileSystemFactory)
  }

  private def toFileStatus(path: String): FileStatus = {
    new FileStatus(10L, true, 1, 100, 100, new Path(path))
  }

  test("getIndexes() returns seq of Indexes") {
    val idx1 = "idx1"
    val idx2 = "idx2"
    val idx3 = "idx3"
    val indexFileStatuses: Seq[FileStatus] = Seq(idx1, idx2, idx3).map(toFileStatus)
    when(mockFileSystem.listStatus(any[Path])).thenReturn(indexFileStatuses.toArray)
    when(mockFileSystem.exists(any[Path])).thenReturn(true)

    def toIndex(str: String): IndexLogEntry = {
      val sourcePlanProperties = SparkPlan.Properties(
        Seq(),
        null,
        null,
        LogicalPlanFingerprint(LogicalPlanFingerprint.Properties(Seq(Signature("", "")))))

      val entry = IndexLogEntry(
        str,
        CoveringIndex(Seq("RGUID"), Seq("Date"), new StructType(), 10, Map()),
        Content(Directory(s"$str/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")),
        Source(SparkPlan(sourcePlanProperties)),
        Map())
      entry.state = Constants.States.ACTIVE
      entry
    }

    val expected = Seq(idx1, idx2, idx3).map(toIndex)
    val actual = indexCollectionManager.getIndexes()
    assert(actual.equals(expected))
  }

  test("delete() throws exception if index is not found") {
    when(mockFileSystem.exists(new Path(systemPath, "idx4"))).thenReturn(false)
    intercept[HyperspaceException](indexCollectionManager.delete("idx4"))
  }

  test("vacuum() throws exception if index is not found") {
    when(mockFileSystem.exists(new Path(systemPath, "idx4"))).thenReturn(false)
    intercept[HyperspaceException](indexCollectionManager.vacuum("idx4"))
  }

  test("restore() throws exception if index is not found") {
    when(mockFileSystem.exists(new Path(systemPath, "idx4"))).thenReturn(false)
    intercept[HyperspaceException](indexCollectionManager.restore("idx4"))
  }

  test("refresh() with mode = 'full' throws exception if index is not found") {
    when(mockFileSystem.exists(new Path(systemPath, "idx4"))).thenReturn(false)
    intercept[HyperspaceException](indexCollectionManager.refresh("idx4", REFRESH_MODE_FULL))
  }

  test("refresh() with mode = 'incremental' throws exception if index is not found") {
    when(mockFileSystem.exists(new Path(systemPath, "idx4"))).thenReturn(false)
    intercept[HyperspaceException](
      indexCollectionManager.refresh("idx4", REFRESH_MODE_INCREMENTAL))
  }
}
