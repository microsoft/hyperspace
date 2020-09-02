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

import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.microsoft.hyperspace.TestUtils
import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}

class IndexLogEntryTest extends SparkFunSuite with SQLHelper {
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

  test("Test files api lists all files from Content object.") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val content = Content.fromDirectory(dirPath)

      val expected = fs.listStatus(dirPath).map(_.getPath).toSet
      val actual = content.files.toSet
      assert(actual.equals(expected))
    }
  }

  test("Test files api lists all files recursively from Content object.") {
    withTempPath { p =>
      // Prepare some files and directories. This time, we make multi-level directory tree.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")
      val d1 = Files.createTempDirectory(dir, "d1")
      val d1Path = PathUtils.makeAbsolute(d1.toString)
      Files.createTempFile(d1, "f4", "")

      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val content = Content.fromDirectory(dirPath)

      val expected =
        (fs.listStatus(dirPath).filter(_.isFile) ++ fs.listStatus(d1Path)).map(_.getPath).toSet

      val actual = content.files.toSet
      assert(actual.equals(expected))
    }
  }

  test("Test fromPath api creates the correct Content object.") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Content object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val fileInfos = fs.listStatus(dirPath).map(FileInfo(_))
      val leafDir = Directory(dirPath.getName, fileInfos)
      val expected = {
        val rootDirectory = TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }

        Content(rootDirectory, NoOpFingerprint())
      }

      // Create actual Content object.
      val actual = Content.fromDirectory(dirPath)

      // Compare.
      assert(actual.equals(expected))
    }
  }

  test("Test fromLeafFiles api creates the correct Content object.") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Content object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val fileStatuses = fs.listStatus(dirPath)
      val fileInfos = fileStatuses.map(FileInfo(_))
      val leafDir = Directory(dirPath.getName, fileInfos)

      val expected = {
        val rootDirectory = TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }

        Content(rootDirectory, NoOpFingerprint())
      }

      // Create actual Content object.
      val actual = Content.fromLeafFiles(fileStatuses)
      assert(actual.equals(expected))
    }
  }

  test("Test fromDir api creates the correct Directory objects, with all files recursively.") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Directory object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val fileInfos = fs.listStatus(dirPath).map(FileInfo(_))
      val leafDir = Directory(dirPath.getName, fileInfos)
      val expected = TestUtils.splitPath(dirPath.getParent).foldLeft(leafDir) { (accum, name) =>
        Directory(name, Seq(), Seq(accum))
      }

      // Create actual Directory object.
      val actual = Directory.fromDirectory(dirPath)

      // Compare.
      assert(actual.equals(expected))
    }
  }
}
