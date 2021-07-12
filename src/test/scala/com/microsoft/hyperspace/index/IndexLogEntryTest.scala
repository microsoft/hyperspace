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

import java.io.File
import java.nio.file
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.microsoft.hyperspace.{BuildInfo, HyperspaceException, TestUtils}
import com.microsoft.hyperspace.index.IndexConstants.UNKNOWN_FILE_ID
import com.microsoft.hyperspace.index.covering.CoveringIndex
import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}

class IndexLogEntryTest extends HyperspaceSuite with SQLHelper {
  var testDir: file.Path = _
  var f1: file.Path = _
  var f2: file.Path = _
  var nestedDir: file.Path = _
  var f3: file.Path = _
  var f4: file.Path = _
  var emptyDir: file.Path = _
  var fs: FileSystem = _
  var fileIdTracker: FileIdTracker = _

  override def beforeAll(): Unit = {
    val testDirPath = Paths.get(inTempDir("testDir"))
    if (Files.exists(testDirPath)) {
      FileUtils.deleteDirectory(new File(inTempDir("testDir")))
    }

    testDir = Files.createDirectories(testDirPath)
    f1 = Files.createFile(Paths.get(testDir + "/f1"))
    f2 = Files.createFile(Paths.get(testDir + "/f2"))
    nestedDir = Files.createDirectories(Paths.get(testDir + "/nested"))
    f3 = Files.createFile(Paths.get(nestedDir + "/f3"))
    f4 = Files.createFile(Paths.get(nestedDir + "/f4"))
    emptyDir = Files.createDirectories(Paths.get(testDir + "/empty"))

    fs = toPath(testDir).getFileSystem(new Configuration)
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(testDir.toFile)
  }

  before {
    fileIdTracker = new FileIdTracker
  }

  private def toPath(path: file.Path): Path = PathUtils.makeAbsolute(path.toString)

  private def toFileStatus(path: file.Path): FileStatus = fs.getFileStatus(toPath(path))

  test("IndexLogEntry spec example") {
    val jsonString =
      s"""
         |{
         |  "name" : "indexName",
         |  "derivedDataset" : {
         |    "type" : "com.microsoft.hyperspace.index.covering.CoveringIndex",
         |    "indexedColumns" : [ "col1" ],
         |    "includedColumns" : [ "col2", "col3" ],
         |    "schema" : {
         |      "type" : "struct",
         |      "fields" : [ {
         |        "name" : "RGUID",
         |        "type" : "string",
         |        "nullable" : true,
         |        "metadata" : { }
         |      } , {
         |        "name" : "Date",
         |        "type" : "string",
         |        "nullable" : true,
         |        "metadata" : { }
         |      } ]
         |    },
         |    "numBuckets" : 200,
         |    "properties" : {}
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
         |                  "name" : "test",
         |                  "files" : [ {
         |                    "name" : "f1",
         |                    "size" : 100,
         |                    "modifiedTime" : 100,
         |                    "id" : 0
         |                  }, {
         |                    "name" : "f2",
         |                    "size" : 100,
         |                    "modifiedTime" : 200,
         |                    "id" : 1
         |                  } ],
         |                  "subDirs" : [ ]
         |                },
         |                "fingerprint" : {
         |                  "kind" : "NoOp",
         |                  "properties" : { }
         |                }
         |              },
         |              "update" : {
         |                "deletedFiles" : {
         |                  "root" : {
         |                    "name" : "",
         |                    "files" : [ {
         |                      "name" : "f1",
         |                      "size" : 10,
         |                      "modifiedTime" : 10,
         |                      "id" : 2
         |                    }],
         |                    "subDirs" : [ ]
         |                  },
         |                  "fingerprint" : {
         |                    "kind" : "NoOp",
         |                    "properties" : { }
         |                  }
         |                },
         |                "appendedFiles" : null
         |              }
         |            },
         |            "kind" : "HDFS"
         |          },
         |          "dataSchema" : {"type":"struct","fields":[]},
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
         |  "properties" : {
         |    "${IndexConstants.HYPERSPACE_VERSION_PROPERTY}" : "${BuildInfo.version}"
         |  },
         |  "version" : "0.1",
         |  "id" : 0,
         |  "state" : "ACTIVE",
         |  "timestamp" : 1578818514080,
         |  "enabled" : true
         |}""".stripMargin

    val schema =
      StructType(Array(StructField("RGUID", StringType), StructField("Date", StringType)))

    val expectedSourcePlanProperties = SparkPlan.Properties(
      Seq(
        Relation(
          Seq("rootpath"),
          Hdfs(Hdfs.Properties(
            Content(Directory(
              "test",
              Seq(FileInfo("f1", 100L, 100L, 0), FileInfo("f2", 100L, 200L, 1)),
              Seq())),
            Some(Update(None, Some(Content(Directory("", Seq(FileInfo("f1", 10, 10, 2))))))))),
          new StructType(),
          "type",
          Map())),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint
          .Properties(Seq(Signature("provider", "signatureValue")))))

    val expected = IndexLogEntry.create(
      "indexName",
      CoveringIndex(Seq("col1"), Seq("col2", "col3"), schema, 200, Map()),
      Content(Directory("rootContentPath")),
      Source(SparkPlan(expectedSourcePlanProperties)),
      Map())
    expected.state = "ACTIVE"
    expected.timestamp = 1578818514080L

    val actual = JsonUtils.fromJson[IndexLogEntry](jsonString)
    assert(actual.equals(expected))
    assert(actual.sourceFilesSizeInBytes == 200L)
  }

  test("Content.files api lists all files from Content object.") {
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
              files = Seq(
                FileInfo("f3", 0, 0, UNKNOWN_FILE_ID),
                FileInfo("f4", 0, 0, UNKNOWN_FILE_ID))))))))

    val expected =
      Seq("file:/a/f1", "file:/a/f2", "file:/a/b/f3", "file:/a/b/f4").map(new Path(_)).toSet
    val actual = content.files.toSet
    assert(actual.equals(expected))
  }

  test("Content.fromDirectory api creates the correct Content object.") {
    val nestedDirPath = toPath(nestedDir)

    val expected = {
      val fileInfos = Seq(f3, f4)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
      val nestedDirDirectory = Directory("nested", fileInfos)
      val rootDirectory = createDirectory(nestedDirPath, nestedDirDirectory)
      Content(rootDirectory, NoOpFingerprint())
    }

    val actual = Content.fromDirectory(nestedDirPath, fileIdTracker, new Configuration)
    assert(contentEquals(actual, expected))
  }

  test("Content.fromLeafFiles api creates the correct Content object.") {
    val nestedDirPath = toPath(nestedDir)

    val expected = {
      val fileInfos = Seq(f3, f4)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
      val nestedDirDirectory = Directory("nested", fileInfos)
      val rootDirectory = createDirectory(nestedDirPath, nestedDirDirectory)
      Content(rootDirectory, NoOpFingerprint())
    }

    val actual = Content.fromDirectory(nestedDirPath, fileIdTracker, new Configuration)
    assert(contentEquals(actual, expected))
  }

  test("Directory.fromDirectory api creates the correct Directory object.") {
    val nestedDirPath = toPath(nestedDir)

    val expected = {
      val fileInfos = Seq(f3, f4)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
      val nestedDirDirectory = Directory("nested", fileInfos)
      createDirectory(nestedDirPath, nestedDirDirectory)
    }

    val actual = Directory.fromDirectory(nestedDirPath, fileIdTracker)
    assert(directoryEquals(actual, expected))
  }

  test(
    "Directory.fromDirectory api creates the correct Directory objects, " +
      "recursively listing all leaf files.") {
    val testDirPath = toPath(testDir)

    val testDirLeafFiles =
      Seq(f1, f2)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val nestedDirLeafFiles =
      Seq(f3, f4)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val testDirDirectory = Directory(
      name = "testDir",
      files = testDirLeafFiles,
      subDirs = Seq(Directory(name = "nested", files = nestedDirLeafFiles)))
    val expected = createDirectory(testDirPath, testDirDirectory)

    val actual = Directory.fromDirectory(testDirPath, fileIdTracker)

    assert(directoryEquals(actual, expected))
  }

  test("Directory.fromLeafFiles api creates the correct Directory object.") {
    val testDirPath = toPath(testDir)

    val testDirLeafFiles =
      Seq(f1, f2)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val nestedDirLeafFiles =
      Seq(f3, f4)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val testDirDirectory = Directory(
      name = "testDir",
      files = testDirLeafFiles,
      subDirs = Seq(Directory(name = "nested", files = nestedDirLeafFiles)))

    val expected = createDirectory(testDirPath, testDirDirectory)

    val actual = Directory.fromLeafFiles(Seq(f1, f2, f3, f4).map(toFileStatus), fileIdTracker)

    assert(directoryEquals(actual, expected))
  }

  test("Directory.fromLeafFiles api does not include other files in the directory.") {
    val testDirPath = toPath(testDir)

    val testDirLeafFiles = Seq(f1)
      .map(toFileStatus)
      .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val nestedDirLeafFiles =
      Seq(f4)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val testDirDirectory = Directory(
      name = "testDir",
      files = testDirLeafFiles,
      subDirs = Seq(Directory(name = "nested", files = nestedDirLeafFiles)))

    val expected = createDirectory(testDirPath, testDirDirectory)

    val actual = Directory.fromLeafFiles(Seq(f1, f4).map(toFileStatus), fileIdTracker)

    assert(directoryEquals(actual, expected))
  }

  test("Directory.fromDirectory where the directory is empty or nonexistent.") {
    val testDirPath = toPath(testDir)
    val emptyDirPath = new Path(testDirPath, "empty")
    val expected = {
      val emptyDirDirectory = Directory(emptyDirPath.getName)
      createDirectory(emptyDirPath, emptyDirDirectory)
    }

    {
      // Test non-existent directory.
      val actual = Directory.fromDirectory(emptyDirPath, fileIdTracker)
      assert(directoryEquals(actual, expected))
    }

    {
      // Test empty directory.
      emptyDirPath.getFileSystem(new Configuration).mkdirs(emptyDirPath)
      val actual = Directory.fromDirectory(emptyDirPath, fileIdTracker)
      assert(directoryEquals(actual, expected))
    }
  }

  test("Directory Test: pathfilter adds only valid files to Directory object.") {
    val testDirPath = toPath(testDir)
    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith("f1")
    }

    val testDirLeafFiles = Seq(f1)
      .map(toFileStatus)
      .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))
    val testDirDirectory = Directory(name = "testDir", files = testDirLeafFiles)
    val expected = createDirectory(testDirPath, testDirDirectory)

    // Create actual Directory object. Path filter should filter only files starting with "f1"
    val actual = Directory.fromDirectory(testDirPath, fileIdTracker, pathFilter)

    assert(directoryEquals(actual, expected))
  }

  test(
    "Directory.fromDirectory and fromLeafFiles where files are at same level but different" +
      "dirs.") {
    // File Structure
    // testDir/temp/a/f1
    // testDir/temp/b/f2

    val tempDir = Files.createDirectories(Paths.get(testDir + "/temp"))
    val a = Files.createDirectories(Paths.get(tempDir + "/a"))
    val b = Files.createDirectories(Paths.get(tempDir + "/b"))
    val f1 = Files.createFile(Paths.get(a + "/f1"))
    val f2 = Files.createFile(Paths.get(b + "/f2"))

    val aDirectory =
      Directory(
        "a",
        Seq(f1)
          .map(toFileStatus)
          .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)))
    val bDirectory =
      Directory(
        "b",
        Seq(f2)
          .map(toFileStatus)
          .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)))
    val tempDirectory = Directory("temp", subDirs = Seq(aDirectory, bDirectory))
    val tempDirectoryPath = toPath(tempDir)

    val expected = createDirectory(tempDirectoryPath, tempDirectory)

    val actual1 = Directory.fromLeafFiles(Seq(f1, f2).map(toFileStatus), fileIdTracker)
    val actual2 = Directory.fromDirectory(toPath(tempDir), fileIdTracker)

    assert(directoryEquals(actual1, expected))
    assert(directoryEquals(actual2, expected))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("Directory.fromDirectory and fromLeafFiles where there is a gap in directories.") {
    // File Structure
    // testDir/temp/a/f1
    // testDir/temp/b/c/f2

    val tempDir = Files.createDirectories(Paths.get(testDir + "/temp"))
    val a = Files.createDirectories(Paths.get(tempDir + "/a"))
    val b = Files.createDirectories(Paths.get(tempDir + "/b"))
    val c = Files.createDirectories(Paths.get(b + "/c"))
    val f1 = Files.createFile(Paths.get(a + "/f1"))
    val f2 = Files.createFile(Paths.get(c + "/f2"))

    val cDirectory =
      Directory(
        "c",
        Seq(f2)
          .map(toFileStatus)
          .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)))
    val bDirectory = Directory("b", subDirs = Seq(cDirectory))
    val aDirectory =
      Directory(
        "a",
        Seq(f1)
          .map(toFileStatus)
          .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)))

    val tempDirectory = Directory("temp", subDirs = Seq(aDirectory, bDirectory))
    val tempDirectoryPath = toPath(tempDir)

    val expected = createDirectory(tempDirectoryPath, tempDirectory)
    val actual1 = Directory.fromLeafFiles(Seq(f1, f2).map(toFileStatus), fileIdTracker)
    val actual2 = Directory.fromDirectory(toPath(tempDir), fileIdTracker)

    assert(directoryEquals(actual1, expected))
    assert(directoryEquals(actual2, expected))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test(
    "Directory.fromDirectory and fromLeafFiles where files belong to multiple" +
      "subdirectories.") {
    // File Structure
    // testDir/temp/a/f1
    // testDir/temp/a/b/f2
    // testDir/temp/a/c/f3

    val tempDir = Files.createDirectories(Paths.get(testDir + "/temp"))
    val a = Files.createDirectories(Paths.get(tempDir + "/a"))
    val b = Files.createDirectories(Paths.get(a + "/b"))
    val c = Files.createDirectories(Paths.get(a + "/c"))
    val f1 = Files.createFile(Paths.get(a + "/f1"))
    val f2 = Files.createFile(Paths.get(b + "/f2"))
    val f3 = Files.createFile(Paths.get(c + "/f3"))

    val bDirectory =
      Directory(
        "b",
        Seq(f2)
          .map(toFileStatus)
          .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)))
    val cDirectory =
      Directory(
        "c",
        Seq(f3)
          .map(toFileStatus)
          .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)))
    val aDirectory = Directory(
      "a",
      Seq(f1)
        .map(toFileStatus)
        .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false)),
      Seq(bDirectory, cDirectory))
    val tempDirectory = Directory("temp", subDirs = Seq(aDirectory))
    val tempDirectoryPath = toPath(tempDir)

    val expected = createDirectory(tempDirectoryPath, tempDirectory)
    val actual1 = Directory.fromLeafFiles(Seq(f1, f2, f3).map(toFileStatus), fileIdTracker)
    val actual2 = Directory.fromDirectory(toPath(a), fileIdTracker)

    assert(directoryEquals(actual1, expected))
    assert(directoryEquals(actual2, expected))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("Directory.merge: test if merge works as expected.") {
    // directory1:
    // a/f1
    // a/f2
    val directory1 = Directory(
      name = "a",
      files = Seq(FileInfo("f1", 100L, 100L, 1L), FileInfo("f2", 100L, 100L, 2L)))

    // directory2:
    // a/b/f3
    // a/b/f4
    val directory2 = Directory(
      name = "a",
      subDirs = Seq(
        Directory(
          name = "b",
          files = Seq(FileInfo("f3", 100L, 100L, 3L), FileInfo("f4", 100L, 100L, 4L)))))

    // Expected result of merging directory1 and directory2:
    // a/f1
    // a/f2
    // a/b/f3
    // a/b/f4
    val expected = Directory(
      name = "a",
      files = Seq(FileInfo("f1", 100L, 100L, 1L), FileInfo("f2", 100L, 100L, 2L)),
      subDirs = Seq(
        Directory(
          name = "b",
          files = Seq(FileInfo("f3", 100L, 100L, 3L), FileInfo("f4", 100L, 100L, 4L)))))

    val actual1 = directory1.merge(directory2)
    val actual2 = directory2.merge(directory1)

    assert(directoryEquals(actual1, expected))
    assert(directoryEquals(actual2, expected))
  }

  test("Directory.merge: test if merge works as expected when directories overlap.") {
    // directory1:
    // a/f1
    // a/f2
    // a/b/f3
    val directory1 = Directory(
      name = "a",
      files = Seq(FileInfo("f1", 100L, 100L, 1L), FileInfo("f2", 100L, 100L, 2L)),
      subDirs = Seq(Directory(name = "b", files = Seq(FileInfo("f3", 100L, 100L, 3L)))))

    // directory2:
    // a/f4
    // a/b/f5
    // a/b/f6
    // a/b/c/f7
    val directory2 = Directory(
      name = "a",
      files = Seq(FileInfo("f4", 100L, 100L, 4L)),
      subDirs = Seq(
        Directory(
          name = "b",
          files = Seq(FileInfo("f5", 100L, 100L, 5L), FileInfo("f6", 100L, 100L, 6L)),
          subDirs = Seq(Directory(name = "c", files = Seq(FileInfo("f7", 100L, 100L, 7L)))))))

    // Expected result of merging directory1 and directory2:
    // directory1:
    // a/f1
    // a/f2
    // a/f4
    // a/b/f3
    // a/b/f5
    // a/b/f6
    // a/b/c/f7
    val expected = Directory(
      name = "a",
      files = Seq(
        FileInfo("f1", 100L, 100L, 1L),
        FileInfo("f2", 100L, 100L, 2L),
        FileInfo("f4", 100L, 100L, 4L)),
      subDirs = Seq(
        Directory(
          name = "b",
          files = Seq(
            FileInfo("f3", 100L, 100L, 3L),
            FileInfo("f5", 100L, 100L, 5L),
            FileInfo("f6", 100L, 100L, 6L)),
          subDirs = Seq(Directory("c", files = Seq(FileInfo("f7", 100L, 100L, 7L)))))))

    val actual1 = directory1.merge(directory2)
    val actual2 = directory2.merge(directory1)

    assert(directoryEquals(actual1, expected))
    assert(directoryEquals(actual2, expected))
  }

  test("Directory.merge: test if exception is thrown when directory names are not equal.") {
    // directory1:
    // a/f1
    // a/f2
    val directory1 = Directory(
      name = "a",
      files = Seq(FileInfo("f1", 100L, 100L, 1L), FileInfo("f2", 100L, 100L, 2L)))

    // directory2:
    // b/f3
    // b/f4
    val directory2 = Directory(
      name = "b",
      files = Seq(FileInfo("f3", 100L, 100L, 3L), FileInfo("f4", 100L, 100L, 4L)))

    val ex1 = intercept[HyperspaceException](directory1.merge(directory2))
    val ex2 = intercept[HyperspaceException](directory2.merge(directory1))

    assert(ex1.msg.contains("Merging directories with names a and b failed."))
    assert(ex2.msg.contains("Merging directories with names b and a failed."))
  }

  private def contentEquals(content1: Content, content2: Content): Boolean = {
    directoryEquals(content1.root, content2.root)
  }

  private def directoryEquals(dir1: Directory, dir2: Directory): Boolean = {
    dir1.name.equals(dir2.name) &&
    dir1.files.toSet.equals(dir2.files.toSet) &&
    dir1.subDirs.size.equals(dir2.subDirs.size) &&
    dir1.subDirs.sortBy(_.name).zip(dir2.subDirs.sortBy(_.name)).forall {
      case (d1, d2) => directoryEquals(d1, d2)
    }
  }

  // Using `directoryPath`, create a Directory tree starting from root and ending at
  // `leafDirectory`.
  private def createDirectory(directoryPath: Path, leafDirectory: Directory): Directory = {
    TestUtils.splitPath(directoryPath.getParent).foldLeft(leafDirectory) { (accum, name) =>
      Directory(name, Seq(), Seq(accum))
    }
  }
}
