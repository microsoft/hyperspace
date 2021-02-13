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
import com.microsoft.hyperspace.index.{Content, FileIdTracker, FileInfo, Hdfs, IndexLogEntry, Relation}
import com.microsoft.hyperspace.index.sources.default.DefaultFileBasedRelation
import com.microsoft.hyperspace.util.PathUtils

/**
 * Implementation for file-based relation used by [[DeltaLakeFileBasedSource]]
 */
class DeltaLakeRelation(spark: SparkSession, override val plan: LogicalRelation)
    extends DefaultFileBasedRelation(spark, plan) {

  /**
   * Computes the signature of the current relation.
   */
  override def signature: String = plan.relation match {
    case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
      location.tableVersion + location.path.toString
  }

  /**
   * All the files that the current relation references to.
   */
  override def allFiles: Seq[FileStatus] = plan.relation match {
    case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
      location
        .getSnapshot(stalenessAcceptable = false)
        .filesForScan(projection = Nil, location.partitionFilters, keepStats = false)
        .files
        .map { f =>
          toFileStatus(f.size, f.modificationTime, new Path(location.path, f.path))
        }
  }

  /**
   * The optional partition base path of the current relation.
   */
  override def partitionBasePath: Option[String] = plan.relation match {
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
        val files = location
          .getSnapshot(stalenessAcceptable = false)
          .filesForScan(projection = Nil, location.partitionFilters, keepStats = false)
          .files
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
          dataSchema.json,
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
   * Returns list of pairs of (file path, file id) to build lineage column.
   *
   * File paths should be the same format as "input_file_name()" of the given relation type.
   * For [[DeltaLakeRelation]], each file path should be in this format:
   *   `file:/path/to/file`
   *
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of pairs of (file path, file id).
   */
  override def lineagePairs(fileIdTracker: FileIdTracker): Seq[(String, Long)] = {
    fileIdTracker.getFileToIdMap.toSeq.map { kv =>
      (kv._1._1, kv._2)
    }
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

  private def deltaLakeVersionHistory(index: IndexLogEntry): Seq[(Int, Int)] = {
    val versions =
      index.derivedDataset.properties.properties
        .getOrElse(DeltaLakeConstants.DELTA_VERSION_HISTORY_PROPERTY, "")

    versions.split(",").reverseIterator.foldLeft(Seq[(Int, Int)]()) { (list, versionStr) =>
      val pair = versionStr.split(":")
      val Array(indexLogVersion, deltaTableVersion) = versionStr.split(":").map(_.toInt)
      if (list.nonEmpty && list.head._2 == deltaTableVersion) {
        // Omit if the delta lake version is the same. In this case, we only take the higher
        // index log version as this is usually from index optimizations.
        list
      } else {
        (indexLogVersion, deltaTableVersion) +: list
      }
    }
  }

  /**
   * Returns IndexLogEntry of the closest index version for the given relation.
   *
   * curFiles is used to calculate the similarity with each index version data.
   * Delta Lake source provider utilizes the history information of DELTA_VERSION_HISTORY_PROPERTY
   * to find the closet version of the index.
   *
   * @param curFiles List of FileInfo for the source files in the relation.
   * @param index Candidate index to be applied.
   * @return IndexLogEntry of the closest version among available index versions.
   */
  override def closestIndexVersion(
      curFiles: Seq[FileInfo],
      index: IndexLogEntry): IndexLogEntry = {
    // Seq of ( index log version, delta lake table version )
    val versions = deltaLakeVersionHistory(index)
    lazy val indexManager = Hyperspace.getContext(spark).indexCollectionManager
    def getIndexLogEntry(logVersion: Int): IndexLogEntry = {
      indexManager
        .getLogManager(index.name)
        .get
        .getLog(logVersion)
        .get
        .asInstanceOf[IndexLogEntry]
    }

    plan.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
        val equalOrLessLastIndex = versions.lastIndexWhere(location.tableVersion >= _._2)
        if (equalOrLessLastIndex == versions.size - 1) {
          // Use the latest version.
          index
        } else if (equalOrLessLastIndex == -1) {
          getIndexLogEntry(versions.head._1)
        } else if (versions(equalOrLessLastIndex)._2 == location.tableVersion) {
          getIndexLogEntry(versions(equalOrLessLastIndex)._1)
        } else {
          val prevPair = versions(equalOrLessLastIndex)
          val nextPair = versions(equalOrLessLastIndex + 1)

          val prevLog = getIndexLogEntry(prevPair._1)
          val nextLog = getIndexLogEntry(nextPair._1)

          val curFilesSize = curFiles.map(_.size).sum

          val prevCommonBytes =
            curFiles.filter(prevLog.sourceFileInfoSet.contains).map(_.size).sum
          val prevAppendedBytes = curFilesSize - prevCommonBytes
          val prevDeletedBytes = prevLog.sourceFilesSizeInBytes - prevCommonBytes

          val nextCommonBytes =
            curFiles.filter(nextLog.sourceFileInfoSet.contains).map(_.size).sum
          val nextAppendedBytes = curFilesSize - nextCommonBytes
          val nextDeletedBytes = nextLog.sourceFilesSizeInBytes - nextCommonBytes

          if ((prevAppendedBytes + prevDeletedBytes) < (nextAppendedBytes + nextDeletedBytes)) {
            prevLog
          } else {
            nextLog
          }
        }
    }
  }
}
