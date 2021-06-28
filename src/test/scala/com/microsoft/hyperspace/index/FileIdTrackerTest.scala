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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.HyperspaceException

class FileIdTrackerTest extends SparkFunSuite {
  test("getMaxFileId returns -1 on a new instance.") {
    val tracker = new FileIdTracker
    assert(tracker.getMaxFileId == -1)
  }

  test("getFileToIdMapping is empty on a new instance.") {
    val tracker = new FileIdTracker
    assert(tracker.getFileToIdMapping.isEmpty)
  }

  test("getFileId returns None on unseen file properties.") {
    val tracker = new FileIdTracker
    assert(tracker.getFileId(path = "abc", size = 123, modifiedTime = 555).isEmpty)
  }

  test("addFileInfo does nothing with an empty files set.") {
    val tracker = new FileIdTracker
    tracker.addFileInfo(Set[FileInfo]())
    assert(tracker.getFileToIdMapping.isEmpty)
    assert(tracker.getMaxFileId == -1)
  }

  test("addFileInfo throws an exception if there is a FileInfo having an invalid file id.") {
    val tracker = new FileIdTracker
    val ex = intercept[HyperspaceException] {
      tracker.addFileInfo(Set(FileInfo("abc", 123, 555, IndexConstants.UNKNOWN_FILE_ID)))
    }
    assert(ex.getMessage.contains("Cannot add file info with unknown id"))
  }

  test(
    "addFileInfo throws an exception if there is a conflict but modifications " +
      "before the exception are retained.") {
    val tracker = new FileIdTracker
    tracker.addFileInfo(Set(FileInfo("def", 123, 555, 10)))
    val ex = intercept[HyperspaceException] {
      implicit def ordering: Ordering[FileInfo] =
        new Ordering[FileInfo] {
          override def compare(x: FileInfo, y: FileInfo): Int = {
            x.name.compareTo(y.name)
          }
        }
      tracker.addFileInfo(
        scala.collection.immutable
          .SortedSet(FileInfo("abc", 100, 555, 15), FileInfo("def", 123, 555, 11)))
    }
    assert(ex.getMessage.contains("Adding file info with a conflicting id"))
    assert(tracker.getFileId("abc", 100, 555).contains(15))
  }

  test("addFileInfo puts new records in the map and increase the max id on success.") {
    val tracker = new FileIdTracker
    tracker.addFileInfo(Set(FileInfo("abc", 123, 555, 10), FileInfo("def", 234, 777, 5)))
    assert(tracker.getFileId("abc", 123, 555).contains(10))
    assert(tracker.getFileId("def", 234, 777).contains(5))
    assert(tracker.getMaxFileId == 10)
  }

  test("addFile returns the existing id and max id is unchanged.") {
    val tracker = new FileIdTracker
    tracker.addFileInfo(Set(FileInfo("abc", 123, 555, 10)))
    assert(tracker.getMaxFileId == 10)
    assert(tracker.addFile(new FileStatus(123, false, 3, 1, 555, new Path("abc"))) == 10)
    assert(tracker.getMaxFileId == 10)
  }

  test("addFile returns a new id and max id is updated.") {
    val tracker = new FileIdTracker
    assert(tracker.addFile(new FileStatus(123, false, 3, 1, 555, new Path("abc"))) == 0)
    assert(tracker.addFile(new FileStatus(123, false, 3, 1, 555, new Path("def"))) == 1)
    assert(tracker.addFile(new FileStatus(124, false, 3, 1, 777, new Path("xyz"))) == 2)
    assert(tracker.getMaxFileId == 2)
    assert(tracker.getFileId("abc", 123, 555).contains(0))
    assert(tracker.getFileId("def", 123, 555).contains(1))
    assert(tracker.getFileId("xyz", 124, 777).contains(2))
  }
}
