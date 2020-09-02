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

import java.io.FileNotFoundException
import java.nio.file
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfter

import com.microsoft.hyperspace.TestUtils
import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}

class IndexLogEntryTest extends SparkFunSuite with SQLHelper with BeforeAndAfter {
  var testDir: file.Path = _
  var f1: file.Path = _
  var f2: file.Path = _
  var nestedDir: file.Path = _
  var f3: file.Path = _
  var f4: file.Path = _
  var fs: FileSystem = _

  override def beforeAll(): Unit = {
    val testDirPath = Paths.get("src/test/resources/testDir")
    if (Files.exists(testDirPath)) {
      Files.delete(testDirPath)
    }

    testDir = Files.createDirectories(Paths.get("src/test/resources/testDir"))
    f1 = Files.createFile(Paths.get(testDir + "/f1"))
    f2 = Files.createFile(Paths.get(testDir + "/f2"))
    nestedDir = Files.createDirectories(Paths.get(testDir + "/nested"))
    f3 = Files.createFile(Paths.get(nestedDir + "/f3"))
    f4 = Files.createFile(Paths.get(nestedDir + "/f4"))
    val dirPath = PathUtils.makeAbsolute(testDir.toString)
    fs = dirPath.getFileSystem(new Configuration)
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(testDir.toFile)
  }

  private def toPath(path: file.Path): Path = PathUtils.makeAbsolute(path.toString)

  private def toFileStatus(path: file.Path): FileStatus = fs.getFileStatus(toPath(path))

  test("IndexLogEntry spec example") {
    val schemaString =
      """{\"type\":\"struct\",
          |\"fields\":[
          |{\"name\":\"RGUID\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},
          |{\"name\":\"Date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}
          |""".stripMargin.replaceAll("\r", "").replaceAll("\n", "")

    val jsonString =
      s"""
        |{
        |  "name" : "indexName",
        |  "derivedDataset" : {
        |    "properties" : {
        |      "columns" : {
        |        "indexed" : [ "col1" ],
        |        "included" : [ "col2", "col3" ]
        |      },
        |      "schemaString" : "$schemaString",
        |      "numBuckets" : 200
        |    },
        |    "kind" : "CoveringIndex"
        |  },
        |  "content" : {
        |    "root" : {
        |      "name" : "rootContentPath",
        |      "files" : [ ],
        |      "subDirs" : [ ]
        |    },
        |    "fingerprint" : {
        |      "kind" : "NoOp",
        |      "properties" : { }
        |    }
        |  },
        |  "source" : {
        |    "plan" : {
        |      "properties" : {
        |        "relations" : [ {
        |          "rootPaths" : [ "rootpath" ],
        |          "data" : {
        |            "properties" : {
        |              "content" : {
        |                "root" : {
        |                  "name" : "",
        |                  "files" : [ {
        |                    "name" : "f1",
        |                    "size" : 100,
        |                    "modifiedTime" : 100
        |                  }, {
        |                    "name" : "f2",
        |                    "size" : 200,
        |                    "modifiedTime" : 200
        |                  } ],
        |                  "subDirs" : [ ]
        |                },
        |                "fingerprint" : {
        |                  "kind" : "NoOp",
        |                  "properties" : { }
        |                }
        |              }
        |            },
        |            "kind" : "HDFS"
        |          },
        |          "dataSchemaJson" : "schema",
        |          "fileFormat" : "type",
        |          "options" : { }
        |        } ],
        |        "rawPlan" : null,
        |        "sql" : null,
        |        "fingerprint" : {
        |          "properties" : {
        |            "signatures" : [ {
        |              "provider" : "provider",
        |              "value" : "signatureValue"
        |            } ]
        |          },
        |          "kind" : "LogicalPlan"
        |        }
        |      },
        |      "kind" : "Spark"
        |    }
        |  },
        |  "extra" : { },
        |  "version" : "0.1",
        |  "id" : 0,
        |  "state" : "ACTIVE",
        |  "timestamp" : 1578818514080,
        |  "enabled" : true
        |}""".stripMargin

    val schema =
      StructType(Array(StructField("RGUID", StringType), StructField("Date", StringType)))

    val expectedSourcePlanProperties = SparkPlan.Properties(
      Seq(Relation(
        Seq("rootpath"),
        Hdfs(Hdfs.Properties(Content(
          Directory("", Seq(FileInfo("f1", 100L, 100L), FileInfo("f2", 200L, 200L)), Seq())))),
        "schema",
        "type",
        Map())),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("provider", "signatureValue")))))

    val expected = IndexLogEntry(
      "indexName",
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(Seq("col1"), Seq("col2", "col3")),
          schema.json,
          200)),
      Content(Directory("rootContentPath")),
      Source(SparkPlan(expectedSourcePlanProperties)),
      Map())
    expected.state = "ACTIVE"
    expected.timestamp = 1578818514080L

    val actual = JsonUtils.fromJson[IndexLogEntry](jsonString)
    assert(actual.equals(expected))
  }

  // CONTENT Tests
  test("Test files api lists all files from Content object.") {
    val dir2Path = PathUtils.makeAbsolute(nestedDir.toString)
    val content = Content.fromDirectory(dir2Path)

    val expected = Seq(f3, f4).map(toPath).toSet
    val actual = content.files.toSet
    assert(actual.equals(expected))
  }

  // CONTENT Tests
  test("Test files api lists all files recursively from Content object.") {
    val dirPath = PathUtils.makeAbsolute(testDir.toString)
    val content = Content.fromDirectory(dirPath)

    val expected = Seq(f1, f2, f3, f4).map(toPath).toSet
    val actual = content.files.toSet
    assert(actual.equals(expected))
  }

  // CONTENT Tests
  test("Test fromDirectory api creates the correct Content object.") {
    val dir2Path = PathUtils.makeAbsolute(nestedDir.toString)

    val fileInfos = Seq(f3, f4).map(toFileStatus).map(FileInfo(_))

    val expected = {
      val leafDir = Directory("nested", fileInfos)
      val rootDirectory = TestUtils.splitPath(dir2Path.getParent).foldLeft(leafDir) {
        (accum, name) =>
          Directory(name, Seq(), Seq(accum))
      }
      Content(rootDirectory, NoOpFingerprint())
    }

    val actual = Content.fromDirectory(dir2Path)
    assert(actual.equals(expected))
  }

  // CONTENT Tests
  test("Test fromLeafFiles api creates the correct Content object.") {
    val dir2Path = PathUtils.makeAbsolute(nestedDir.toString)
    val fileStatuses = Seq(f3, f4).map(toFileStatus)

    val expected = {
      val fileInfos = fileStatuses.map(FileInfo(_))
      val leafDir = Directory("nested", fileInfos)
      val rootDirectory = TestUtils.splitPath(dir2Path.getParent).foldLeft(leafDir) {
        (accum, name) =>
          Directory(name, Seq(), Seq(accum))
      }
      Content(rootDirectory, NoOpFingerprint())
    }

    val actual = Content.fromDirectory(dir2Path)
    assert(actual.equals(expected))
  }

  // Directory Tests
  test("Test fromDirectory api creates the correct Directory object.") {
    val dir2Path = PathUtils.makeAbsolute(nestedDir.toString)

    val fileInfos = Seq(f3, f4).map(toFileStatus).map(FileInfo(_))

    val expected = {
      val leafDir = Directory("nested", fileInfos)
      TestUtils.splitPath(dir2Path.getParent).foldLeft(leafDir) {
        (accum, name) =>
          Directory(name, Seq(), Seq(accum))
      }
    }

    val actual = Directory.fromDirectory(dir2Path)
    assert(actual.equals(expected))
  }

  // Directory Tests
  test("Test fromDirectory api creates the correct Directory objects, " +
    "recursively listing all leaf files.") {
    val dirPath = PathUtils.makeAbsolute(testDir.toString)
    val nestedDirPath = PathUtils.makeAbsolute(nestedDir.toString)

    val testDirLeafFiles = Seq(f1, f2).map(toFileStatus).map(FileInfo(_))
    val nestedLeafFiles = Seq(f3, f4).map(toFileStatus).map(FileInfo(_))
    val leafDir1 = Directory(name = "testDir", files = testDirLeafFiles)
    val leafDir2 = Directory(name = "nested", files = nestedLeafFiles)
    val expected2 = TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir1) { (accum, name) =>
      Directory(name, Seq(), Seq(accum))
    }
    val expected1 = TestUtils.splitPath(nestedDirPath.getParent)
      .foldLeft(leafDir2) { (accum, name) =>
      Directory(name, Seq(), Seq(accum))
    }
    val expected = Directory(
      expected1.name,
      subDirs = Seq(expected1.subDirs.head, expected2.subDirs.head)
    )

    val actual = Directory.fromDirectory(dirPath)

    assert(actual.equals(expected))
  }

  // Directory Tests
  test("Test fromLeafFiles api creates the correct Directory object.") {
    withTempPath { p =>
      // Prepare some files and directories.
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Directory object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fileStatuses = fs.listStatus(dirPath)
      val fileInfos = fileStatuses.map(FileInfo(_))

      val expected = {
        val leafDir = Directory(dirPath.getName, fileInfos)
         TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }
      }

      // Create actual Directory object.
      val actual = Directory.fromLeafFiles(fileStatuses)
      assert(actual.equals(expected))
    }
  }

  // Directory Tests
  test("Test fromLeafFiles api does not include other files in the directory.") {
    withTempPath { p =>
      // Prepare some files and directories.
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Directory object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)

      // Remove some files from available files on the disk. These shouldn't be in Directory.
      val fileStatuses = fs.listStatus(dirPath).take(2)
      val fileInfos = fileStatuses.map(FileInfo(_))

      val expected = {
        val leafDir = Directory(dirPath.getName, fileInfos)
        TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }
      }

      // Create actual Directory object.
      val actual = Directory.fromLeafFiles(fileStatuses)

      // Assert that a Directory object created from subset of files only contains those files.
      assert(actual.equals(expected))
    }
  }

  // Directory Tests
  test("Test throwIfNotExist flag throws exception for non-existent directory, " +
    "otherwise works as expected.") {
    withTempPath { p =>
      // No files/directories exist at this point.
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val nonExistentDir = new Path(dirPath, "nonexistent")

      // Try create Directory object with throwIfNotExists to true. This should throw exception.
      intercept[FileNotFoundException] {
        Directory.fromDirectory(nonExistentDir, throwIfNotExists = true)
      }

      // Try create Directory object with throwifNotExists to false. This should create empt
      // Directory.
      val expected = {
        val leafDir = Directory(nonExistentDir.getName)
        TestUtils.splitPath(nonExistentDir.getParent).foldLeft(leafDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }
      }

      val actual = Directory.fromDirectory(nonExistentDir, throwIfNotExists = false)
      assert(actual.equals(expected))
    }
  }

  // Directory Tests
  test("Test pathfilter adds only valid files to Directory object.") {
    withTempPath { p =>
      // Prepare some files and directories.
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      val pathFilter = new PathFilter {
        override def accept(path: Path): Boolean = path.getName.startsWith("f1")
      }

      // Create expected Directory object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fileInfos = fs
        .listStatus(dirPath)
        .filter(_.getPath.getName.startsWith("f1"))
        .map(FileInfo(_))
      val expected = {
        val leafDir = Directory(dirPath.getName, fileInfos)
        TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }
      }

      // Create actual Directory object.
      val actual = Directory.fromDirectory(dirPath, pathFilter)

      // Compare.
      assert(actual.equals(expected))
    }
  }
}
