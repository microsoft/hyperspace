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

import java.io.{File, FileNotFoundException}
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
      FileUtils.deleteDirectory(new File("src/test/resources/testDir"))
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

  test("Content.files api lists all files from Content object.") {
    val content = Content(Directory("file:/", subDirs =
      Seq(
        Directory("a",
          files = Seq(FileInfo("f1", 0, 0), FileInfo("f2", 0, 0)),
          subDirs = Seq(
            Directory("b",
              files = Seq(FileInfo("f3", 0, 0), FileInfo("f4", 0, 0)))))
      )))

    val expected =
      Seq("file:/a/f1", "file:/a/f2", "file:/a/b/f3", "file:/a/b/f4").map(new Path(_)).toSet
    val actual = content.files.toSet
    assert(actual.equals(expected))
  }

  test("Content.fromDirectory api creates the correct Content object.") {
    val nestedDirPath = PathUtils.makeAbsolute(nestedDir.toString)

    val fileInfos = Seq(f3, f4).map(toFileStatus).map(FileInfo(_))

    val expected = {
      val leafDir = Directory("nested", fileInfos)
      val rootDirectory = TestUtils.splitPath(nestedDirPath.getParent).foldLeft(leafDir) {
        (accum, name) =>
          Directory(name, Seq(), Seq(accum))
      }
      Content(rootDirectory, NoOpFingerprint())
    }

    val actual = Content.fromDirectory(nestedDirPath)
    assert(actual.equals(expected))
  }

  test("Content.fromLeafFiles api creates the correct Content object.") {
    val nestedDirPath = PathUtils.makeAbsolute(nestedDir.toString)
    val fileStatuses = Seq(f3, f4).map(toFileStatus)

    val expected = {
      val fileInfos = fileStatuses.map(FileInfo(_))
      val leafDir = Directory("nested", fileInfos)
      val rootDirectory = TestUtils.splitPath(nestedDirPath.getParent).foldLeft(leafDir) {
        (accum, name) =>
          Directory(name, Seq(), Seq(accum))
      }
      Content(rootDirectory, NoOpFingerprint())
    }

    val actual = Content.fromDirectory(nestedDirPath)
    assert(actual.equals(expected))
  }

  test("Directory.fromDirectory api creates the correct Directory object.") {
    val nestedDirPath = PathUtils.makeAbsolute(nestedDir.toString)

    val fileInfos = Seq(f3, f4).map(toFileStatus).map(FileInfo(_))

    val expected = {
      val leafDir = Directory("nested", fileInfos)
      TestUtils.splitPath(nestedDirPath.getParent).foldLeft(leafDir) {
        (accum, name) =>
          Directory(name, Seq(), Seq(accum))
      }
    }

    val actual = Directory.fromDirectory(nestedDirPath)
    assert(actual.equals(expected))
  }

  test("Directory Test: fromDirectory api creates the correct Directory objects, " +
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

  test("Directory.fromLeafFiles api creates the correct Directory object.") {
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

    val actual = Directory.fromLeafFiles(Seq(f1, f2, f3, f4).map(toFileStatus))

    assert(actual.equals(expected))
  }

  test("Directory.fromLeafFiles api does not include other files in the directory.") {
    val dirPath = PathUtils.makeAbsolute(testDir.toString)
    val nestedDirPath = PathUtils.makeAbsolute(nestedDir.toString)

    val testDirLeafFiles = Seq(f1).map(toFileStatus).map(FileInfo(_))
    val nestedLeafFiles = Seq(f4).map(toFileStatus).map(FileInfo(_))
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

    val actual = Directory.fromLeafFiles(Seq(f1, f4).map(toFileStatus))

    assert(actual.equals(expected))
  }

  test("Directory.fromLeafFiles: throwIfNotExist flag throws exception for non-existent" +
    "directory, otherwise works as expected.") {
    val dirPath = PathUtils.makeAbsolute(testDir.toString)
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

  test("Directory Test: pathfilter adds only valid files to Directory object.") {
    val dirPath = PathUtils.makeAbsolute(testDir.toString)
    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith("f1")
    }

    val testDirLeafFiles = Seq(f1).map(toFileStatus).map(FileInfo(_))
    val leafDir1 = Directory(name = "testDir", files = testDirLeafFiles)
    val expected = TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir1) { (accum, name) =>
      Directory(name, Seq(), Seq(accum))
    }

    // Create actual Directory object. Path filter should filter only files starting with "f1"
    val actual = Directory.fromDirectory(dirPath, pathFilter)

    // Compare.
    assert(actual.equals(expected))
  }
}
