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

package com.microsoft.hyperspace

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import com.microsoft.hyperspace.MockEventLogger.reset
import com.microsoft.hyperspace.index.{FileIdTracker, IndexConfigTrait, IndexConstants, IndexLogEntry, IndexLogManager, IndexLogManagerFactoryImpl}
import com.microsoft.hyperspace.telemetry.{EventLogger, HyperspaceEvent}
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

object TestUtils {
  def copyWithState(index: IndexLogEntry, state: String): IndexLogEntry = {
    val result = index.copy()
    result.state = state
    result
  }

  /**
   * Split path into its segments and return segment names as a sequence.
   * For e.g. a path "file:/C:/d1/d2/d3/f1.parquet" will return
   * Seq("f1.parquet", "d3", "d2", "d1", "file:/C:/")
   *
   * @param path Path to split into segments.
   * @return Segments as a seq.
   */
  def splitPath(path: Path): Seq[String] = {
    if (path.getParent == null) {
      // `path` is now root. It's getName returns "" but toString returns actual path,
      // E.g. "file:/C:/" for Windows.
      Seq(path.toString)
    } else {
      path.getName +: splitPath(path.getParent)
    }
  }

  /**
   * Delete files from a given path.
   *
   * @param path Path to the folder containing files.
   * @param pattern File name pattern to delete.
   * @param numFilesToDelete Number of files to delete.
   * @return Deleted files' FileStatus.
   */
  def deleteFiles(path: String, pattern: String, numFilesToDelete: Int): Seq[FileStatus] = {
    val pathToDelete = new Path(path, pattern)
    val files = pathToDelete
      .getFileSystem(new Configuration)
      .globStatus(pathToDelete)

    assert(files.length >= numFilesToDelete)
    val filesToDelete = files.take(numFilesToDelete)
    filesToDelete.foreach(f => FileUtils.delete(f.getPath))

    filesToDelete
  }

  def logManager(systemPath: Path, indexName: String): IndexLogManager = {
    val indexPath = PathUtils.makeAbsolute(s"$systemPath/$indexName")
    IndexLogManagerFactoryImpl.create(indexPath)
  }

  def latestIndexLogEntry(systemPath: Path, indexName: String): IndexLogEntry = {
    logManager(systemPath, indexName)
      .getLatestStableLog()
      .get
      .asInstanceOf[IndexLogEntry]
  }

  def getFileIdTracker(systemPath: Path, indexConfig: IndexConfigTrait): FileIdTracker = {
    latestIndexLogEntry(systemPath, indexConfig.indexName).fileIdTracker
  }
}

/**
 * This class can be used to test emitted events from Hyperspace actions.
 */
class MockEventLogger extends EventLogger {
  import com.microsoft.hyperspace.MockEventLogger.emittedEvents
  // Reset events.
  reset()

  override def logEvent(event: HyperspaceEvent): Unit = {
    emittedEvents :+= event
  }
}

object MockEventLogger {
  var emittedEvents: Seq[HyperspaceEvent] = Seq()

  def reset(): Unit = {
    emittedEvents = Seq()
  }
}

object TestConfig {
  val HybridScanEnabled = Seq(
    IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
    IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "0.99",
    IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "0.99")

  val HybridScanEnabledAppendOnly = Seq(
    IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
    IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "0.99",
    IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "0")
}
