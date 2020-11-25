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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, Relation}
import com.microsoft.hyperspace.index.sources.{FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}
import com.microsoft.hyperspace.util.PathUtils

/**
 * Delta Lake file-based source provider.
 *
 * This source can support relations that meet the following criteria:
 *   - The relation is [[HadoopFsRelation]] with [[TahoeLogFileIndex]] as file index.
 */
class DeltaLakeFileBasedSource(private val spark: SparkSession) extends FileBasedSourceProvider {
  val DELTA_FORMAT_STR = "delta"

  /**
   * Creates [[Relation]] for IndexLogEntry using the given [[LogicalRelation]].
   *
   * @param logicalRelation logical relation to derive [[Relation]] from.
   * @return [[Relation]] object if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def createRelation(
      logicalRelation: LogicalRelation,
      fileIdTracker: FileIdTracker): Option[Relation] = {
    logicalRelation.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, dataSchema, _, _, options) =>
        val files = location
          .getSnapshot(stalenessAcceptable = false)
          .filesForScan(projection = Nil, location.partitionFilters, keepStats = false)
          .files
          .map { f =>
            new FileStatus(
              /* length */ f.size,
              /* isDir */ false,
              /* blockReplication */ 0,
              /* blockSize */ 1,
              /* modificationTime */ f.modificationTime,
              new Path(location.path, f.path))
          }
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties =
          Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
        val fileFormatName = "delta"
        // "path" key in options can incur multiple data read unexpectedly and keep
        // the table version info as metadata.
        val opts = options - "path" + ("versionAsOf" -> location.tableVersion.toString)
        Some(
          Relation(
            Seq(PathUtils.makeAbsolute(location.path).toString),
            Hdfs(sourceDataProperties),
            dataSchema.json,
            fileFormatName,
            opts))
      case _ => None
    }
  }

  /**
   * Given a [[Relation]], returns a new [[Relation]] that will have the latest source.
   *
   * @param relation [[Relation]] object to reconstruct [[DataFrame]] with.
   * @return [[Relation]] object if the given 'relation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def refreshRelation(relation: Relation): Option[Relation] = {
    if (relation.fileFormat.equals(DELTA_FORMAT_STR)) {
      Some(relation.copy(options = relation.options - "versionAsOf" - "timestampAsOf"))
    } else {
      None
    }
  }

  /**
   * Computes the signature using the given [[LogicalRelation]]. This computes a signature of
   * using version info and table name.
   *
   * @param logicalRelation logical relation to compute signature from.
   * @return Signature computed if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def signature(logicalRelation: LogicalRelation): Option[String] = {
    logicalRelation.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
        Some(location.tableVersion + location.path.toString)
      case _ => None
    }
  }

  /**
   * Retrieves all input files from the given [[LogicalRelation]].
   *
   * @param logicalRelation Logical relation to retrieve input files from.
   * @return List of [[FileStatus]] for the given relation.
   */
  override def allFiles(logicalRelation: LogicalRelation): Option[Seq[FileStatus]] = {
    logicalRelation.relation match {
      case HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _) =>
        val files = location
          .getSnapshot(stalenessAcceptable = false)
          .filesForScan(projection = Nil, location.partitionFilters, keepStats = false)
          .files
          .map { f =>
            new FileStatus(
              /* length */ f.size,
              /* isDir */ false,
              /* blockReplication */ 0,
              /* blockSize */ 1,
              /* modificationTime */ f.modificationTime,
              new Path(location.path, f.path))
          }
        Some(files)
      case _ => None
    }
  }

  /**
   * Constructs the basePath for the given [[FileIndex]].
   *
   * @param location Partitioned data location.
   * @return basePath to read the given partitioned location.
   */
  override def partitionBasePath(location: FileIndex): Option[String] = {
    location match {
      case d: TahoeLogFileIndex =>
        Some(d.path.toString)
      case _ =>
        None
    }
  }

  /**
   * Returns list of pairs of (file path, file id) to build lineage column.
   *
   * File paths should be the same format with "input_file_name()" of the given relation type.
   *
   * @param logicalRelation Logical relation to check the relation type.
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of pairs of (file path, file id).
   */
  override def lineagePairs(
      logicalRelation: LogicalRelation,
      fileIdTracker: FileIdTracker): Option[Seq[(String, Long)]] = {
    logicalRelation.relation match {
      case HadoopFsRelation(_: TahoeLogFileIndex, _, _, _, _, _) =>
        Some(fileIdTracker.getFileToIdMap.toSeq.map { kv =>
          (kv._1._1, kv._2)
        })
      case _ =>
        None
    }
  }
}

/**
 * Builder for building [[DeltaLakeFileBasedSource]].
 */
class DeltaLakeFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new DeltaLakeFileBasedSource(spark)
}
