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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, Relation}
import com.microsoft.hyperspace.index.sources.{FileBasedRelation, FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}
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
}

/**
 * Delta Lake file-based source provider.
 *
 * This source can support relations that meet the following criteria:
 *   - The relation is [[HadoopFsRelation]] with [[TahoeLogFileIndex]] as file index.
 */
class DeltaLakeFileBasedSource(private val spark: SparkSession) extends FileBasedSourceProvider {
  private val DELTA_FORMAT_STR = "delta"

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
   * Returns a file format name to read internal data files for a given [[Relation]].
   *
   * @param relation [[Relation]] object to read internal data files.
   * @return File format to read internal data files.
   */
  override def internalFileFormatName(relation: Relation): Option[String] = {
    if (relation.fileFormat.equals(DELTA_FORMAT_STR)) {
      Some("parquet")
    } else {
      None
    }
  }

  /**
   * Returns true if the given logical plan is a relation for Delta Lake.
   *
   * @param plan Logical plan to check if it's supported.
   * @return Some(true) if the given plan is a supported relation, otherwise None.
   */
  def isSupportedRelation(plan: LogicalPlan): Option[Boolean] = plan match {
    case LogicalRelation(HadoopFsRelation(_: TahoeLogFileIndex, _, _, _, _, _), _, _, _) =>
      Some(true)
    case _ => None
  }

  /**
   * Returns the [[FileBasedRelation]] that wraps the given logical plan if the given
   * logical plan is a supported relation.
   *
   * @param plan Logical plan to wrap to [[FileBasedRelation]]
   * @return [[FileBasedRelation]] that wraps the given logical plan.
   */
  def getRelation(plan: LogicalPlan): Option[FileBasedRelation] = plan match {
    case l @ LogicalRelation(HadoopFsRelation(_: TahoeLogFileIndex, _, _, _, _, _), _, _, _) =>
      Some(new DeltaLakeRelation(spark, l))
    case _ => None
  }
}

/**
 * Builder for building [[DeltaLakeFileBasedSource]].
 */
class DeltaLakeFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new DeltaLakeFileBasedSource(spark)
}
