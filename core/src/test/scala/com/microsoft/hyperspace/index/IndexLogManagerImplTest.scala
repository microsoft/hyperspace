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
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.microsoft.hyperspace.index.IndexConstants.HYPERSPACE_LOG
import com.microsoft.hyperspace.util.{FileUtils, JsonUtils}
import com.microsoft.hyperspace.{SparkInvolvedSuite, TestUtils}

@RunWith(classOf[JUnitRunner])
class IndexLogManagerImplTest extends FunSuite with SparkInvolvedSuite with BeforeAndAfterAll {
  val testRoot = "src/test/resources/indexLogManagerTests"
  val sampleIndexLogEntry: IndexLogEntry = IndexLogEntry(
    "entityName",
    CoveringIndex(
      CoveringIndex.Properties(
        CoveringIndex.Properties.Columns(Seq("id"), Seq("name", "school")),
        "id INT name STRING school STRING",
        100)),
    Content(
      "/root/log",
      Seq(
        Content.Directory("dir1", Seq("1.json", "2.json"), NoOpFingerprint()),
        Content.Directory("dir2", Seq("1.json", "2.json"), NoOpFingerprint()))),
    Source(
      SparkPlan(
        SparkPlan.Properties(
          rawPlan = "spark plan",
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(Seq(Signature("provider", "signature")))))),
      Seq(
        Hdfs(properties = Hdfs.Properties(content = Content(
          "/root/data",
          Seq(
            Content.Directory("dir1", Seq("1.json", "2.json"), NoOpFingerprint()),
            Content.Directory("dir2", Seq("1.json", "2.json"), NoOpFingerprint()))))))),
    Map())

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.delete(new Path(testRoot), true)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(testRoot), true)
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

    def getEntry(state: String): LogEntry = {
      TestUtils.copyWithState(sampleIndexLogEntry, state)
    }

    FileUtils.createFile(
      fs,
      new Path(path, s"$HYPERSPACE_LOG/0"),
      JsonUtils.toJson(getEntry("CREATING")))
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
  }

  test("testUpdateLatestStableLog passes if latestStable.json can be created") {
    val path = new Path(testRoot, UUID.randomUUID().toString)
    val fs = path.getFileSystem(new Configuration)
    FileUtils.createFile(fs, new Path(path, s"$HYPERSPACE_LOG/0"), "file contents")
    new IndexLogManagerImpl(path).createLatestStableLog(0)
    assert(fs.exists(new Path(path, s"$HYPERSPACE_LOG/latestStable")))
  }

  // TODO: Test the case where the id does not exist.
  // TODO: Test file rename failure if possible.
}
