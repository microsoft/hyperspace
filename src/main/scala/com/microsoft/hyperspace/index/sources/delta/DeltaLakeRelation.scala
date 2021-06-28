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

package com.microsoft.hyperspace.index.sources.delta

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, IndexLogEntry, Relation}
import com.microsoft.hyperspace.index.sources.default.DefaultFileBasedRelation
import com.microsoft.hyperspace.util.{HyperspaceConf, PathUtils}

/**
 * Implementation for file-based relation used by [[DeltaLakeFileBasedSource]]
 */
class DeltaLakeRelation(spark: SparkSession, override val plan: LogicalRelation)
    extends DefaultFileBasedRelation(spark, plan) {

  /**
   * Computes the signature of the current relation.
   */
  override def signature: String =
    plan.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
        location.tableVersion + location.path.toString
    }

  /**
   * All the files that the current relation references to.
   */
  lazy override val allFiles: Seq[FileStatus] = plan.relation match {
    case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
      DeltaLakeShims
        .getFiles(location)
        .map { f =>
          toFileStatus(f.size, f.modificationTime, new Path(location.path, f.path))
        }
  }

  /**
   * The optional partition base path of the current relation.
   */
  override def partitionBasePath: Option[String] =
    plan.relation match {
      case HadoopFsRelation(t: TahoeLogFileIndex, _, _, _, _, _) if t.partitionSchema.nonEmpty =>
        Some(t.path.toString)
      case _ => None
    }

  /**
   * Creates [[Relation]] for IndexLogEntry using the current relation.
   *
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object that describes the current relation.
   */
  override def createRelationMetadata(fileIdTracker: FileIdTracker): Relation = {
    plan.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, dataSchema, _, _, options) =>
        val files = DeltaLakeShims
          .getFiles(location)
          .map { f =>
            toFileStatus(f.size, f.modificationTime, new Path(location.path, f.path))
          }
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties =
          Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
        val fileFormatName = "delta"

        // Use case-sensitive map if the provided options are case insensitive.
        val caseSensitiveOptions = options match {
          case map: CaseInsensitiveMap[String] => map.originalMap
          case map => map
        }

        val basePathOpt = partitionBasePath.map("basePath" -> _)

        // "path" key in options can incur multiple data read unexpectedly and keep
        // the table version info as metadata.
        val opts = caseSensitiveOptions - "path" +
          ("versionAsOf" -> location.tableVersion.toString) ++ basePathOpt

        Relation(
          Seq(
            PathUtils
              .makeAbsolute(location.path.toString, spark.sessionState.newHadoopConf())
              .toString),
          Hdfs(sourceDataProperties),
          dataSchema,
          fileFormatName,
          opts)
    }
  }

  /**
   * Returns whether the current relation has parquet source files or not.
   *
   * @return Always true since delta lake files are stored as Parquet.
   */
  override def hasParquetAsSourceFormat: Boolean = true

  /**
   * For [[DeltaLakeRelation]], each file path should be in this format:
   *   `file:/path/to/file`
   */
  override def pathNormalizer: String => String = {
    identity
  }

  private def toFileStatus(fileSize: Long, modificationTime: Long, path: Path): FileStatus = {
    new FileStatus(
      /* length */ fileSize,
      /* isDir */ false,
      /* blockReplication */ 0,
      /* blockSize */ 1,
      /* modificationTime */ modificationTime,
      path)
  }

  /**
   * Retrieve the history of index log version and delta table version from the given index.
   *
   * @param index Index to retrieve the version info.
   * @return List of (index log version, delta table version) in ascending order.
   */
  private def deltaLakeVersionHistory(index: IndexLogEntry): Seq[(Int, Int)] = {
    // Versions are comma separated - <index log version>:<delta table version>.
    // e.g. "1:2,3:5,5:9"
    val versions =
      index.derivedDataset.properties
        .getOrElse(DeltaLakeConstants.DELTA_VERSION_HISTORY_PROPERTY, "")

    if (versions.nonEmpty) {
      // Versions are processed in a reverse order to keep the higher index log version
      // in case different index log versions refer to the same delta lake version.
      // For example, "1:1,2:2,3:2" will become Seq((1, 1), (3, 2)).
      versions.split(",").reverseIterator.foldLeft(Seq[(Int, Int)]()) { (acc, versionStr) =>
        val Array(indexLogVersion, deltaTableVersion) = versionStr.split(":").map(_.toInt)
        if (acc.nonEmpty && acc.head._2 == deltaTableVersion) {
          // Omit if the delta lake version is the same. In this case, we only take the higher
          // index log version as it's from index optimizations.
          acc
        } else {
          (indexLogVersion, deltaTableVersion) +: acc
        }
      }
    } else {
      Seq()
    }
  }

  /**
   * Returns IndexLogEntry of the closest version for the given index.
   *
   * Delta Lake source provider utilizes the history information of DELTA_VERSION_HISTORY_PROPERTY
   * to find the closet version of the index.
   *
   * @param index Candidate index to be applied.
   * @return IndexLogEntry of the closest version among available index versions.
   */
  override def closestIndex(index: IndexLogEntry): IndexLogEntry = {
    // Only support when Hybrid Scan is enabled for both appended and deleted files.
    // TODO: Support time travel utilizing Hybrid Scan append-only.
    //   See https://github.com/microsoft/hyperspace/issues/408.
    if (!(HyperspaceConf.hybridScanEnabled(spark) &&
        HyperspaceConf.hybridScanDeleteEnabled(spark) &&
        index.derivedDataset.canHandleDeletedFiles)) {
      return index
    }

    // Seq of (index log version, delta lake table version)
    val versionHistory = deltaLakeVersionHistory(index)
    if (versionHistory.isEmpty) {
      return index
    }

    lazy val indexManager = Hyperspace.getContext(spark).indexCollectionManager
    def getIndexLogEntry(logVersion: Int): IndexLogEntry = {
      indexManager
        .getIndex(index.name, logVersion)
        .get
        .asInstanceOf[IndexLogEntry]
    }

    val activeLogVersions =
      indexManager.getIndexVersions(index.name, Seq(Constants.States.ACTIVE))
    val versions = versionHistory.filter {
      case (indexLogVersion, _) => activeLogVersions.contains(indexLogVersion)
    }

    plan.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
        // Find the largest index version whose delta table version is equal or less than
        // the given relation.
        val equalOrLessThanLastIndex = versions.lastIndexWhere(location.tableVersion >= _._2)
        if (equalOrLessThanLastIndex == versions.size - 1) {
          // The given table version is equal or larger than the latest index's.
          // Use the latest version.
          getIndexLogEntry(versions.last._1)
        } else if (equalOrLessThanLastIndex == -1) {
          // The given table version is smaller than the version at index creation.
          // Use the initial version.
          getIndexLogEntry(versions.head._1)
        } else if (versions(equalOrLessThanLastIndex)._2 == location.tableVersion) {
          // There is the index version that built for the given table version.
          // Use the exact version.
          getIndexLogEntry(versions(equalOrLessThanLastIndex)._1)
        } else {
          // Now, there are 2 candidate versions for the given table version:
          // prevPair(indexVer1, tablePrevVersion), nextPair(indexVer2, tableNextVersion)
          // which is (tablePrevVersion < TimeTravel table version < tableNextVersion).
          val prevPair = versions(equalOrLessThanLastIndex)
          val nextPair = versions(equalOrLessThanLastIndex + 1)

          val prevLog = getIndexLogEntry(prevPair._1)
          val nextLog = getIndexLogEntry(nextPair._1)

          def getDiffBytes(logEntry: IndexLogEntry): Long = {
            val commonBytes =
              allFileInfos.filter(logEntry.sourceFileInfoSet.contains).map(_.size).sum
            // appended bytes + deleted bytes
            (allFileSizeInBytes - commonBytes) + (logEntry.sourceFilesSizeInBytes - commonBytes)
          }

          // Prefer index with less diff bytes to reduce the chance of regression from Hybrid Scan.
          if (getDiffBytes(prevLog) < getDiffBytes(nextLog)) {
            prevLog
          } else {
            nextLog
          }
        }
    }
  }
}
