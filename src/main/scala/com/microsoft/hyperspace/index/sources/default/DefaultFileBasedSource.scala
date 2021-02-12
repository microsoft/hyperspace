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

package com.microsoft.hyperspace.index.sources.default

import java.util.Locale

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, Relation}
import com.microsoft.hyperspace.index.IndexConstants.GLOBBING_PATTERN_KEY
import com.microsoft.hyperspace.index.sources.{FileBasedRelation, FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}
import com.microsoft.hyperspace.util.{CacheWithTransform, HashingUtils, HyperspaceConf}

/**
 * Implementation for file-based relation used by [[DefaultFileBasedSource]]
 */
class DefaultFileBasedRelation(spark: SparkSession, override val plan: LogicalRelation)
    extends FileBasedRelation {
  override def options: Map[String, String] = plan.relation.asInstanceOf[HadoopFsRelation].options

  /**
   * Computes the signature of the current relation.
   */
  override def signature: String = plan.relation match {
    case HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _) =>
      val result = filesFromIndex(location).sortBy(_.getPath.toString).foldLeft("") {
        (acc: String, f: FileStatus) =>
          HashingUtils.md5Hex(acc + fingerprint(f))
      }
      result
  }

  /**
   * All the files that the current relation references to.
   */
  override def allFiles: Seq[FileStatus] = plan.relation match {
    case HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _) =>
      filesFromIndex(location)
  }

  /**
   * The partition schema of the current relation.
   */
  override def partitionSchema: StructType = plan.relation match {
    case HadoopFsRelation(location: FileIndex, _, _, _, _, _) =>
      location.partitionSchema
  }

  /**
   * The optional partition base path of the current relation.
   */
  override def partitionBasePath: Option[String] = plan.relation match {
    case HadoopFsRelation(p: PartitioningAwareFileIndex, _, _, _, _, _)
        if p.partitionSpec.partitions.nonEmpty =>
      // For example, we could have the following in PartitionSpec:
      //   - partition columns = "col1", "col2"
      //   - partitions: "/path/col1=1/col2=1", "/path/col1=1/col2=2", etc.
      // , and going up the same number of directory levels as the number of partition columns
      // will compute the base path. Note that PartitionSpec.partitions will always contain
      // all the partitions in the path, so "partitions.head" is taken as an initial value.
      val basePath = p.partitionSpec.partitionColumns
        .foldLeft(p.partitionSpec.partitions.head.path)((path, _) => path.getParent)
      Some(basePath.toString)
    case _ => None
  }

  /**
   * Creates [[HadoopFsRelation]] based on the current relation.
   *
   * This is mainly used in conjunction with [[createLogicalRelation]].
   */
  override def createHadoopFsRelation(
      location: FileIndex,
      dataSchema: StructType,
      options: Map[String, String]): HadoopFsRelation = plan.relation match {
    case h: HadoopFsRelation =>
      h.copy(location = location, dataSchema = dataSchema, options = options)(spark)
  }

  /**
   * Creates [[LogicalRelation]] based on the current relation.
   *
   * This is mainly used to read the index files.
   */
  override def createLogicalRelation(
      hadoopFsRelation: HadoopFsRelation,
      newOutput: Seq[AttributeReference]): LogicalRelation = {
    plan.copy(relation = hadoopFsRelation, output = newOutput)
  }

  /**
   * Creates [[Relation]] for IndexLogEntry for the current relation.
   *
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object that describes the current relation.
   */
  override def createRelationMetadata(fileIdTracker: FileIdTracker): Relation = {
    plan.relation match {
      case HadoopFsRelation(
          location: PartitioningAwareFileIndex,
          _,
          dataSchema,
          _,
          fileFormat,
          options) =>
        val files = filesFromIndex(location)
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties =
          Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
        val fileFormatName = fileFormat.asInstanceOf[DataSourceRegister].shortName

        // Use case-sensitive map if the provided options are case insensitive. Case-insensitive
        // map converts all key-value pairs to lowercase before storing them in the metadata,
        // making them unusable for future use. An example is "basePath" option.
        val caseSensitiveOptions = options match {
          case map: CaseInsensitiveMap[String] => map.originalMap
          case map => map
        }

        // Get basePath of hive-partitioned data sources, if applicable.
        val basePathOpt = partitionBasePath.map("basePath" -> _)

        // "path" key in options can incur multiple data read unexpectedly.
        val opts = caseSensitiveOptions - "path" ++ basePathOpt

        val rootPaths = opts.get(GLOBBING_PATTERN_KEY) match {
          case Some(pattern) =>
            // Validate if globbing pattern matches actual source paths.
            // This logic is picked from the globbing logic at:
            // https://github.com/apache/spark/blob/v2.4.4/sql/core/src/main/scala/org/apache/
            // spark/sql/execution/datasources/DataSource.scala#L540
            val fs = filesFromIndex(location).head.getPath
              .getFileSystem(spark.sessionState.newHadoopConf())
            val globPaths = pattern
              .split(",")
              .map(_.trim)
              .map { path =>
                val hdfsPath = new Path(path)
                val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
                qualified.toString -> SparkHadoopUtil.get.globPathIfNecessary(fs, qualified)
              }
              .toMap

            val globPathValues = globPaths.values.flatten.toSet
            if (!location.rootPaths.forall(globPathValues.contains)) {
              throw HyperspaceException(
                "Some glob patterns do not match with available root " +
                  s"paths of the source data. Please check if $pattern matches all of " +
                  s"${location.rootPaths.mkString(",")}.")
            }
            globPaths.keySet.toSeq

          case _ => location.rootPaths.map(_.toString)
        }

        Relation(rootPaths, Hdfs(sourceDataProperties), dataSchema.json, fileFormatName, opts)
    }
  }

  private def fingerprint(fileStatus: FileStatus): String = {
    fileStatus.getLen.toString + fileStatus.getModificationTime.toString +
      fileStatus.getPath.toString
  }

  private def filesFromIndex(index: PartitioningAwareFileIndex): Seq[FileStatus] = {
    try {
      // Keep the `asInstanceOf` to force casting or fallback because Databricks
      // `InMemoryFileIndex` implementation returns `SerializableFileStatus` instead of the
      // standard API's `FileStatus`.
      index.allFiles.map(_.asInstanceOf[FileStatus])
    } catch {
      case e: ClassCastException if e.getMessage.contains("SerializableFileStatus") =>
        val dbClassName = "org.apache.spark.sql.execution.datasources.SerializableFileStatus"
        val clazz = Utils.classForName(dbClassName)
        val lengthMethod = clazz.getMethod("length")
        val isDirMethod = clazz.getMethod("isDir")
        val modificationTimeMethod = clazz.getMethod("modificationTime")
        val pathMethod = clazz.getMethod("path")
        index.allFiles.asInstanceOf[Seq[_]].map { f =>
          new FileStatus(
            lengthMethod.invoke(f).asInstanceOf[Long],
            isDirMethod.invoke(f).asInstanceOf[Boolean],
            0,
            0,
            modificationTimeMethod.invoke(f).asInstanceOf[Long],
            new Path(pathMethod.invoke(f).asInstanceOf[String]))
        }
    }
  }

  /**
   * Returns whether the current relation has parquet source files or not.
   *
   * @return True if source files of the current relation are parquet.
   */
  def hasParquetAsSourceFormat: Boolean = plan.relation match {
    case h: HadoopFsRelation =>
      h.fileFormat.asInstanceOf[DataSourceRegister].shortName.equals("parquet")
  }

  /**
   * Returns list of pairs of (file path, file id) to build lineage column.
   *
   * File paths should be the same format as "input_file_name()" of the given relation type.
   * For [[DefaultFileBasedRelation]], each file path should be in this format:
   *   `file:///path/to/file`
   *
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of pairs of (file path, file id).
   */
  override def lineagePairs(fileIdTracker: FileIdTracker): Seq[(String, Long)] = {
    fileIdTracker.getFileToIdMap.toSeq.map { kv =>
      (kv._1._1.replace("file:/", "file:///"), kv._2)
    }
  }
}

/**
 * Default implementation for file-based Spark built-in sources such as parquet, csv, json, etc.
 *
 * This source can support relations that meet the following criteria:
 *   - The relation is [[HadoopFsRelation]] with [[PartitioningAwareFileIndex]] as file index.
 *   - Its file format implements [[DataSourceRegister]].
 */
class DefaultFileBasedSource(private val spark: SparkSession) extends FileBasedSourceProvider {
  private val supportedFormats: CacheWithTransform[String, Set[String]] =
    new CacheWithTransform[String, Set[String]]({ () =>
      HyperspaceConf.supportedFileFormatsForDefaultFileBasedSource(spark)
    }, { formats =>
      formats.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
    })

  /**
   * Returns true if the given [[FileFormat]] is supported, false otherwise.
   *
   * @param format [[FileFormat]] object.
   * @return true if the given [[FileFormat]] is supported, false otherwise.
   */
  private def isSupportedFileFormat(format: FileFormat): Boolean = {
    format match {
      case d: DataSourceRegister if isSupportedFileFormatName(d.shortName) => true
      case _ => false
    }
  }

  /**
   * Returns true if the given format name is supported, false otherwise.
   *
   * @param name File format name (e.g, parquet, csv, json, etc.).
   * @return true if the given format name is supported, false otherwise.
   */
  private def isSupportedFileFormatName(name: String): Boolean = {
    supportedFormats.load().contains(name.toLowerCase(Locale.ROOT))
  }

  /**
   * Given a [[Relation]], returns a new [[Relation]] that will have the latest source.
   *
   * @param relation [[Relation]] object to reconstruct [[DataFrame]] with.
   * @return [[Relation]] object if the given 'relation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def refreshRelation(relation: Relation): Option[Relation] = {
    if (isSupportedFileFormatName(relation.fileFormat)) {
      // No change is needed because rootPaths will be pointing to the latest source files.
      Some(relation)
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
    if (isSupportedFileFormatName(relation.fileFormat)) {
      // Same as original file format.
      Some(relation.fileFormat)
    } else {
      None
    }
  }

  /**
   * Returns true if the given logical plan is a supported relation.
   *
   * @param plan Logical plan to check if it's supported.
   * @return Some(true) if the given plan is a supported relation, otherwise None.
   */
  def isSupportedRelation(plan: LogicalPlan): Option[Boolean] = plan match {
    case LogicalRelation(
        HadoopFsRelation(_: PartitioningAwareFileIndex, _, _, _, fileFormat, _),
        _,
        _,
        _) if isSupportedFileFormat(fileFormat) =>
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
    case l @ LogicalRelation(
          HadoopFsRelation(_: PartitioningAwareFileIndex, _, _, _, fileFormat, _),
          _,
          _,
          _) if isSupportedFileFormat(fileFormat) =>
      Some(new DefaultFileBasedRelation(spark, l))
    case _ => None
  }
}

/**
 * Builder for building [[DefaultFileBasedSource]].
 */
class DefaultFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new DefaultFileBasedSource(spark)
}
