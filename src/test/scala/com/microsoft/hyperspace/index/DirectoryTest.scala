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

import com.microsoft.hyperspace.TestUtils
import com.microsoft.hyperspace.util.PathUtils

class DirectoryTest extends SparkFunSuite with SQLHelper {
  test("testFromDir") {
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
      val bottomDir = Directory(dirPath.getName, fileInfos)
      val expected = TestUtils.splitPath(dirPath.getParent).foldLeft(bottomDir) { (accum, name) =>
        Directory(name, Seq(), Seq(accum))
      }

      // Create actual Directory object.
      val actual = Directory.fromPath(dirPath)

      // Compare.
      assert(actual.equals(expected))
    }
  }
}
