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

  test("testFiles") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir: Path = Files.createDirectories(Paths.get(p.getAbsolutePath))
      val f1 = Files.createTempFile(dir, "f1", "")
      val f2 = Files.createTempFile(dir, "f2", "")
      val f3 = Files.createTempFile(dir, "f3", "")

      val path = PathUtils.makeAbsolute(dir.toString)

      val content = Content.fromPath(path)

      val expected = Seq(f1, f2, f3).map(f => PathUtils.makeAbsolute(f.toString))
      val actual = content.files
      assert(actual.equals(expected))
    }
  }

  test("testFromPath") {
    withTempPath { p =>
      // Prepare some files and directories.
      Files.createDirectories(Paths.get(p.getAbsolutePath))
      val dir: Path = Files.createDirectories(Paths.get(p.getAbsolutePath))
      val f1 = Files.createTempFile(dir, "f1", "")
      val f2 = Files.createTempFile(dir, "f2", "")
      val f3 = Files.createTempFile(dir, "f3", "")

      val path = PathUtils.makeAbsolute(dir.toString)

      // Create expected Directory object.
      val fs = path.getFileSystem(new Configuration)
      val fileInfos =
        Array(f1, f2, f3)
          .map(f => PathUtils.makeAbsolute(f.toString))
          .map(fs.getFileStatus)
          .map(FileInfo(_))
      val bottomDir = Directory(path.getName, fileInfos)

      val expected = {
        val rooDirectory = TestUtils.splitPath(path.getParent).foldLeft(bottomDir) {
          (accum, name) =>
            Directory(name, Seq(), Seq(accum))
        }

        Content(rooDirectory, NoOpFingerprint())
      }

      // Create actual Directory object.
      val actual = Content.fromPath(path)

      // scalastyle:off
      println("testing build failures")
      println("actual")
      println(actual)
      println(JsonUtils.toJson(actual))

      println("expected")
      println(expected)
      println(JsonUtils.toJson(expected))
      // Compare.
      assert(actual.equals(expected))
    }
  }

  test("testFromLeafFiles") {}

}
