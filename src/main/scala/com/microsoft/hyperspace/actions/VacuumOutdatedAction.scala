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

package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.Path

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, VACUUMINGOUTDATED}
import com.microsoft.hyperspace.index.{IndexConstants, IndexDataManager, IndexLogEntry, IndexLogManager, LogEntry}
import com.microsoft.hyperspace.index.sources.delta.DeltaLakeRelationMetadata
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, VacuumOutdatedActionEvent}
import com.microsoft.hyperspace.util.FileUtils

/**
 * Vacuum outdated data of indexes.
 *
 * Algorithm:
 *  - Delete every version except the latest versions.
 */
class VacuumOutdatedAction(
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends Action {
  private lazy val previousIndexLogEntry = {
    logManager.getLog(baseId) match {
      case Some(e: IndexLogEntry) => e
      case _ =>
        throw HyperspaceException("LogEntry must exist for vacuum outdated operation.")
    }
  }

  final override lazy val logEntry: LogEntry = {
    previousIndexLogEntry.relations match {
      case null => previousIndexLogEntry

      case relations if relations.nonEmpty =>
        val relationMetadata = Hyperspace
          .getContext(spark)
          .sourceProviderManager
          .getRelationMetadata(relations.head)

        val updatedDerivedDataset = relationMetadata match {
          case deltaLakeRelationMetadata: DeltaLakeRelationMetadata =>
            // Reset Delta Lake version mapping.
            val resetProperty = deltaLakeRelationMetadata.resetDeltaVersionHistory(
              previousIndexLogEntry.derivedDataset.properties)

            val newProperty = deltaLakeRelationMetadata.enrichIndexProperties(
              resetProperty + (IndexConstants.INDEX_LOG_VERSION -> endId.toString))

            previousIndexLogEntry.derivedDataset.withNewProperties(newProperty)
          case _ => previousIndexLogEntry.derivedDataset
        }
        previousIndexLogEntry.copy(derivedDataset = updatedDerivedDataset)

      case _ => previousIndexLogEntry
    }
  }

  override def transientState: String = VACUUMINGOUTDATED

  override def finalState: String = ACTIVE

  override def validate(): Unit = {
    if (!previousIndexLogEntry.state.equalsIgnoreCase(ACTIVE)) {
      throw HyperspaceException(
        s"VacuumOutdated is only supported in $ACTIVE state. " +
          s"Current state is ${previousIndexLogEntry.state}.")
    }
  }

  final override def op(): Unit = {
    // Delete unused directory first, then delete unused files in used directories.
    val indexVersionsInUse: Set[Int] = logEntry match {
      case indexLogEntry: IndexLogEntry =>
        dataVersionInfos(indexLogEntry)

      case other =>
        throw HyperspaceException(
          s"VacuumOutdated is not supported for log entry class ${other.getClass.getName}")
    }

    // Delete version directories not used.
    dataManager.getAllVersionIds().foreach { id =>
      if (!indexVersionsInUse.contains(id)) {
        dataManager.delete(id)
      }
    }

    val filesInUse = logEntry match {
      case indexLogEntry: IndexLogEntry =>
        indexLogEntry.content.fileInfos.map { info =>
          info.name
        }
    }

    // Delete unused files.
    dataManager.getAllFilePaths().foreach { path =>
      // Ignore files such as "_SUCCESS" and "._SUCCESS.crc".
      if (!path.getName.startsWith("_") &&
        !path.getName.startsWith("._") &&
        !filesInUse.contains(path.toString)) {
        FileUtils.delete(path)
      }
    }
  }

  /**
   * Extracts latest versions of an index.
   *
   * @return List of directory paths containing index files for latest index version.
   */
  private[actions] def dataVersionInfos(entry: IndexLogEntry): Set[Int] = {
    // Get used versions using the filenames of contents.
    // length + 1 due to '=' between prefix and version number.
    val prefixLength = IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX.length + 1
    entry
      .indexDataDirectoryPaths()
      .map(dirname => new Path(dirname).getName)
      .collect {
        case name if name.startsWith(IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX) =>
          name.drop(prefixLength).toInt
      }
      .toSet
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    VacuumOutdatedActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
