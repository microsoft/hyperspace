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

import java.util.UUID

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.internal.Logging

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.util.{FileUtils, JsonUtils}

/**
 * Interface for IndexLogManager which handles log operations.
 */
trait IndexLogManager {
  def getLog(id: Int): Option[LogEntry]

  def getLatestId(): Option[Int]

  final def getLatestLog(): Option[LogEntry] = getLatestId().flatMap(getLog)

  /** Returns the latest LogEntry whose state is STABLE */
  def getLatestStableLog(): Option[LogEntry]

  /** Returns index log versions whose state is in the given states */
  def getIndexVersions(states: Seq[String]): Seq[Int]

  /** update latest.json symlink to the given id/path */
  def createLatestStableLog(id: Int): Boolean

  /** delete latestStable.json */
  def deleteLatestStableLog(): Boolean

  /** write contents of log to id.json using optimistic concurrency. retrun false if fail */
  def writeLog(id: Int, log: LogEntry): Boolean
}

class IndexLogManagerImpl(indexPath: Path, hadoopConfiguration: Configuration = new Configuration)
    extends IndexLogManager
    with Logging {
  // Use FileContext instead of FileSystem for atomic renames?
  private lazy val fs: FileSystem = indexPath.getFileSystem(hadoopConfiguration)

  private lazy val hyperspaceLogPath: Path = new Path(indexPath, IndexConstants.HYPERSPACE_LOG)

  private val pathFromId: Int => Path = id => new Path(hyperspaceLogPath, id.toString)

  private val LATEST_STABLE_LOG_NAME = "latestStable"

  private val latestStablePath = new Path(hyperspaceLogPath, LATEST_STABLE_LOG_NAME)

  private def getLog(path: Path): Option[LogEntry] = {
    if (!fs.exists(path)) {
      return None
    }

    val contents = FileUtils.readContents(fs, path)

    try {
      Some(LogEntry.fromJson(contents))
    } catch {
      case e: Exception =>
        throw HyperspaceException(
          s"Cannot parse JSON in ${path}: ${e.getMessage}")
    }
  }

  override def getLog(id: Int): Option[LogEntry] = {
    getLog(pathFromId(id))
  }

  override def getLatestId(): Option[Int] = {
    if (!fs.exists(hyperspaceLogPath)) {
      return None
    }
    val ids = fs.listStatus(hyperspaceLogPath).collect {
      case file: FileStatus if Try(file.getPath.getName.toInt).toOption.isDefined =>
        file.getPath.getName.toInt
    }
    if (ids.isEmpty) None else Some(ids.max)
  }

  override def getLatestStableLog(): Option[LogEntry] = {
    val latestStableLogPath = new Path(hyperspaceLogPath, LATEST_STABLE_LOG_NAME)

    val log = getLog(latestStableLogPath)
    if (log.isEmpty) {
      val idOpt = getLatestId()
      if (idOpt.isDefined) {
        (idOpt.get to 0 by -1).foreach { id =>
          val entry = getLog(id)
          if (entry.exists(e => Constants.STABLE_STATES.contains(e.state))) {
            return entry
          }
          if (entry.exists(e =>
              e.state.equals(Constants.States.CREATING)
                || e.state.equals(Constants.States.VACUUMING))) {
            // Do not consider unrelated logs before creating or vacuuming state.
            return None
          }
        }
      }
      None
    } else {
      assert(Constants.STABLE_STATES.contains(log.get.state))
      log
    }
  }

  override def getIndexVersions(states: Seq[String]): Seq[Int] = {
    val latestId = getLatestId()
    if (latestId.isDefined) {
      (latestId.get to 0 by -1).map { id =>
        getLog(id) match {
          case Some(entry) if states.contains(entry.state) =>
            Some(id)
          case _ => None
        }
      }.flatten
    } else {
      Seq()
    }
  }

  override def createLatestStableLog(id: Int): Boolean = {
    getLog(id) match {
      case Some(logEntry) if Constants.STABLE_STATES.contains(logEntry.state) =>
        Try(
          FileUtil
            .copy(fs, pathFromId(id), fs, latestStablePath, false, hadoopConfiguration)) match {
          case Success(v) => v
          case Failure(e) =>
            logError(s"Failed to create the latest stable log with id = '$id'", e)
            false
        }
      case Some(logEntry) =>
        logError(s"Found LogEntry with non stable state = ${logEntry.state} for id = '$id'")
        false
      case None =>
        logError(s"Unable to get LogEntry for id = '$id'")
        false
    }
  }

  override def deleteLatestStableLog(): Boolean = {
    try {
      if (!fs.exists(latestStablePath)) {
        true
      } else {
        fs.delete(latestStablePath, true)
      }
    } catch {
      case ex: Exception =>
        logError("Failed to delete the latest stable log", ex)
        false
    }
  }

  override def writeLog(id: Int, log: LogEntry): Boolean = {
    if (fs.exists(pathFromId(id))) {
      false
    } else {
      try {
        val tempPath = new Path(hyperspaceLogPath, "temp" + UUID.randomUUID())
        FileUtils.createFile(fs, tempPath, JsonUtils.toJson(log))

        // Atomic rename: if rename fails, someone else succeeded in writing id json
        fs.rename(tempPath, pathFromId(id))
      } catch {
        case ex: Exception =>
          logError(s"Failed to write log with id = '$id'", ex)
          false
      }
    }
  }
}
