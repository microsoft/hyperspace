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

import java.nio.file.{Files, Path, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper

import com.microsoft.hyperspace.TestUtils
import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}

class ContentTest extends SparkFunSuite with SQLHelper {

  test("test files api") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val content = Content.fromPath(dirPath)

      val expected = fs.listStatus(dirPath).map(_.getPath).toSet
      val actual = content.files.toSet
      assert(actual.equals(expected))
    }
  }

  test("test fromPath api") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir: Path = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Content object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val fileInfos = fs.listStatus(dirPath).map(FileInfo(_))
      val bottomDir = Directory(dirPath.getName, fileInfos)
      val expected = {
        val rooDirectory = TestUtils.splitPath(dirPath.getParent).foldLeft(bottomDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }

        Content(rooDirectory, NoOpFingerprint())
      }

      // Create actual Content object.
      val actual = Content.fromPath(dirPath)

      // Compare.
      assert(actual.equals(expected))
    }
  }

  test("test fromLeafFiles api") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir: Path = Files.createDirectories(Paths.get(p.getAbsolutePath))
      Files.createTempFile(dir, "f1", "")
      Files.createTempFile(dir, "f2", "")
      Files.createTempFile(dir, "f3", "")

      // Create expected Content object.
      val dirPath = PathUtils.makeAbsolute(dir.toString)
      val fs = dirPath.getFileSystem(new Configuration)
      val fileStatuses = fs.listStatus(dirPath)
      val fileInfos = fileStatuses.map(FileInfo(_))
      val bottomDir = Directory(dirPath.getName, fileInfos)

      val expected = {
        val rooDirectory = TestUtils.splitPath(dirPath.getParent).foldLeft(bottomDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }

        Content(rooDirectory, NoOpFingerprint())
      }

      // Create actual Content object.
      val actual = Content.fromLeafFiles(fileStatuses)
      assert(actual.equals(expected))
    }
  }
}
