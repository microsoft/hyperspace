/*
 * Copyright (2021) The Hyperspace Project Authors.
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
import org.scalatest.BeforeAndAfterEach

import com.microsoft.hyperspace.HyperspaceException

class FileIdTrackerTest extends SparkFunSuite with BeforeAndAfterEach {
  var tracker: FileIdTracker = _

  override def beforeEach(): Unit = {
    tracker = new FileIdTracker
  }

  test("getMaxFileId returns -1 on a new instance") {
    assert(tracker.getMaxFileId == -1)
  }

  test("getFileToIdMap is empty on a new instance") {
    assert(tracker.getFileToIdMap.isEmpty)
  }

  test("getFileId returns None on unseen file properties") {
    assert(tracker.getFileId(path = "abc", size = 123, modifiedTime = 666).isEmpty)
  }

  test("addFileInfo does nothing with an empty files set") {
    tracker.addFileInfo(Set[FileInfo]())
    assert(tracker.getFileToIdMap.isEmpty)
    assert(tracker.getMaxFileId == -1)
  }

  test("addFileInfo throws an exception if there is a FileInfo having an invalid file id") {
    assertThrows[HyperspaceException] {
      tracker.addFileInfo(Set(FileInfo("abc", 123, 666, IndexConstants.UNKNOWN_FILE_ID)))
    }
  }

  test(
    "addFileInfo throws an exception if there is a conflict but modifications " +
      "before the exception are retained") {
    tracker.getFileToIdMap.put(("def", 123, 666), 10)
    assertThrows[HyperspaceException] {
      implicit def ordering: Ordering[FileInfo] = new Ordering[FileInfo] {
        override def compare(x: FileInfo, y: FileInfo): Int = {
          x.name.compareTo(y.name)
        }
      }
      tracker.addFileInfo(scala.collection.immutable.SortedSet(
        FileInfo("abc", 100, 666, 15),
        FileInfo("def", 123, 666, 11)))
    }
    assert(tracker.getFileToIdMap.get("abc", 100, 666).contains(15))
  }

  test("addFileInfo puts new records in the map and increase the max id on success") {
    tracker.addFileInfo(Set(FileInfo("abc", 123, 666, 10), FileInfo("def", 234, 777, 5)))
    assert(tracker.getFileToIdMap.get("abc", 123, 666).contains(10))
    assert(tracker.getFileToIdMap.get("def", 234, 777).contains(5))
    assert(tracker.getMaxFileId == 10)
  }

  test("addFile returns the existing id and max id is unchanged") {
    tracker.getFileToIdMap.put(("abc", 123, 666), 10)
    assert(tracker.addFile(new FileStatus(123, false, 3, 1, 666, new Path("abc"))) == 10)
    assert(tracker.getMaxFileId == -1)
  }

  test("addFile returns a new id and max id is updated") {
    assert(tracker.addFile(new FileStatus(123, false, 3, 1, 666, new Path("abc"))) == 0)
    assert(tracker.addFile(new FileStatus(123, false, 3, 1, 666, new Path("def"))) == 1)
    assert(tracker.addFile(new FileStatus(124, false, 3, 1, 777, new Path("xyz"))) == 2)
    assert(tracker.getMaxFileId == 2)
    assert(tracker.getFileToIdMap.get("abc", 123, 666).contains(0))
    assert(tracker.getFileToIdMap.get("def", 123, 666).contains(1))
    assert(tracker.getFileToIdMap.get("xyz", 124, 777).contains(2))
  }
}
