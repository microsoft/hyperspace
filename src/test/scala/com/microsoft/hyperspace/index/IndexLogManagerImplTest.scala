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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.TestUtils
import com.microsoft.hyperspace.index.IndexConstants.HYPERSPACE_LOG
import com.microsoft.hyperspace.index.covering.CoveringIndex
import com.microsoft.hyperspace.util.{FileUtils, JsonUtils}

class IndexLogManagerImplTest extends HyperspaceSuite {
  val testRoot = inTempDir("indexLogManagerTests")
  val sampleIndexLogEntry: IndexLogEntry = IndexLogEntry(
    "entityName",
    CoveringIndex(
      Seq("id"),
      Seq("name", "school"),
      StructType(
        StructField("id", IntegerType) ::
          StructField("name", StringType) ::
          StructField("school", StringType) ::
          Nil),
      100,
      Map()),
    Content(
      Directory(
        "/root/log",
        files = Seq(),
        subDirs = Seq(
          Directory(
            "dir1",
            Seq(FileInfo("1.json", 100L, 200L, 1L), FileInfo("2.json", 100L, 200L, 2L))),
          Directory(
            "dir2",
            Seq(FileInfo("1.json", 100L, 200L, 3L), FileInfo("2.json", 100L, 200L, 4L)))))),
    Source(
      SparkPlan(SparkPlan.Properties(
        Seq(Relation(
          Seq("rootpath"),
          Hdfs(properties = Hdfs.Properties(content = Content(Directory(
            "/root/data",
            files = Seq(),
            subDirs = Seq(
              Directory(
                "dir1",
                Seq(FileInfo("1.json", 100L, 200L, 5L), FileInfo("2.json", 100L, 200L, 6L))),
              Directory(
                "dir2",
                Seq(FileInfo("1.json", 100L, 200L, 7L), FileInfo("2.json", 100L, 200L, 8L)))))))),
          new StructType(),
          "type",
          Map())),
        null,
        null,
        LogicalPlanFingerprint(
          LogicalPlanFingerprint.Properties(Seq(Signature("provider", "signature"))))))),
    Map())

  private def getEntry(state: String): LogEntry = {
    TestUtils.copyWithState(sampleIndexLogEntry, state)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.delete(new Path(testRoot), isRecursive = true)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(testRoot), isRecursive = true)
    super.afterAll()
  }

  test("testGetLog returns None if log not found") {
    val path = new Path(testRoot, "testPath")
    assert(new IndexLogManagerImpl(path).getLog(0).isEmpty)
  }

  test("testGetLog returns IndexLogEntry if id found") {
    val path = new Path(testRoot, "testPath")
    FileUtils.createFile(
      path.getFileSystem(new Configuration),
      new Path(path, s"$HYPERSPACE_LOG/0"),
      JsonUtils.toJson(sampleIndexLogEntry))
    val actual = new IndexLogManagerImpl(path).getLog(0).get
    val expected = sampleIndexLogEntry
    assert(actual.equals(expected))
  }

  test("testGetLog fails with Exception if json is not in proper form") {
    val path = new Path(testRoot, "testPath")

    // find position to insert \0
    val jsonContent = JsonUtils.toJson(sampleIndexLogEntry)
    val sourceIndex = jsonContent.indexOf("\"source\"")
    val damagedJsonContent = jsonContent.substring(0, sourceIndex + 8) + "\u0000" + jsonContent
      .substring(sourceIndex + 8);

    FileUtils.createFile(
      path.getFileSystem(new Configuration),
      new Path(path, s"$HYPERSPACE_LOG/0"),
      damagedJsonContent)

    assertThrows[HyperspaceException](new IndexLogManagerImpl(path).getLog(0).get)
  }

  test("testGetLog for path") {}

  test("testWriteNextLog") {}

  test("testGetLatestStableLog") {}

  test("testGetLatestLog") {}

  test("testDeleteLatestStableLog") {}

  test("testWriteLog pass if no other file exists with same name") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val entry = sampleIndexLogEntry
    assert(new IndexLogManagerImpl(path).writeLog(0, entry))
    assert(!new IndexLogManagerImpl(path).writeLog(0, entry))
  }

  test("testGetLatestId") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)
    FileUtils.createFile(fs, new Path(path, s"$HYPERSPACE_LOG/0"), "file contents")
    FileUtils.createFile(fs, new Path(path, s"$HYPERSPACE_LOG/1"), "file contents")
    FileUtils.createFile(fs, new Path(path, s"$HYPERSPACE_LOG/abc"), "file contents")
    FileUtils.createFile(fs, new Path(path, s"$HYPERSPACE_LOG/20"), "file contents")

    val expected = Some(20)
    val actual = new IndexLogManagerImpl(path).getLatestId()
    assert(actual.equals(expected))
  }

  test("testGetLatestStableLog returns latest stable log") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)

    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/0"),
      JsonUtils.toJson(getEntry("CREATING")))
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/1"),
      JsonUtils.toJson(getEntry("ACTIVE")))
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/3"),
      JsonUtils.toJson(getEntry("ACTIVE")))
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/4"),
      JsonUtils.toJson(getEntry("REFRESHING")))
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/20"),
      JsonUtils.toJson(getEntry("CANCELLING")))

    val expected = Some(getEntry("ACTIVE"))
    val actual = new IndexLogManagerImpl(path).getLatestStableLog()
    assert(actual.equals(expected))
    val actualActiveVersions = new IndexLogManagerImpl(path).getIndexVersions(Seq("ACTIVE"))
    assert(actualActiveVersions.equals(Seq(3, 1)))
  }

  test("testGetLatestStableLog shouldn't return irrelevant previous log.") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)

    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/8"),
      JsonUtils.toJson(getEntry("ACTIVE")))
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/10"),
      JsonUtils.toJson(getEntry("VACUUMING")))

    {
      val actual = new IndexLogManagerImpl(path).getLatestStableLog()
      assert(actual.isEmpty)
    }

    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/12"),
      JsonUtils.toJson(getEntry("CREATING")))

    {
      val actual = new IndexLogManagerImpl(path).getLatestStableLog()
      assert(actual.isEmpty)
    }
  }

  test("testUpdateLatestStableLog passes if latestStable.json can be created") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/0"),
      JsonUtils.toJson(getEntry("ACTIVE")))
    val result = new IndexLogManagerImpl(path).createLatestStableLog(0)
    assert(result === true)
    assert(fs.exists(new Path(path, s"$HYPERSPACE_LOG/latestStable")))
  }

  test("testUpdateLatestStableLog fails if log state is not stable") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)
    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/0"),
      JsonUtils.toJson(getEntry("CANCELLING")))
    val result = new IndexLogManagerImpl(path).createLatestStableLog(0)
    assert(result === false)
    assert(!fs.exists(new Path(path, s"$HYPERSPACE_LOG/latestStable")))
  }

  test("testUpdateLatestStableLog fails with exception if unable to find a valid log entry") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)
    FileUtils.createFile(fs, new Path(path, s"$HYPERSPACE_LOG/0"), "Invalid Log Entry")
    assertThrows[HyperspaceException](new IndexLogManagerImpl(path).createLatestStableLog(0))
  }

  // TODO: Test the case where the id does not exist.
  // TODO: Test file rename failure if possible.
}
