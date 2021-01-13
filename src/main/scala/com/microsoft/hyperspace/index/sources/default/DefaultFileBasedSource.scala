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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, Relation}
import com.microsoft.hyperspace.index.IndexConstants.GLOBBING_PATTERN_KEY
import com.microsoft.hyperspace.index.sources.{FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}
import com.microsoft.hyperspace.util.{CacheWithTransform, HashingUtils, HyperspaceConf}

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

  private def getLogicalRelation(plan: LogicalPlan): Option[LogicalRelation] =
    plan match {
      case logicalRelation: LogicalRelation => Some(logicalRelation)
      case _ => None
    }

  /**
   * Creates [[Relation]] for IndexLogEntry using the given [[LogicalRelation]].
   *
   * @param logicalPlan Logical plan to derive [[Relation]] from.
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def createRelation(
      logicalPlan: LogicalPlan,
      fileIdTracker: FileIdTracker): Option[Relation] = {
    getLogicalRelation(logicalPlan).map(_.relation).flatMap {
      case HadoopFsRelation(
          location: PartitioningAwareFileIndex,
          _,
          dataSchema,
          _,
          fileFormat,
          options) if isSupportedFileFormat(fileFormat) =>
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
        val basePathOpt = partitionBasePath(logicalPlan).flatten.map("basePath" -> _)

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

        Some(
          Relation(rootPaths, Hdfs(sourceDataProperties), dataSchema.json, fileFormatName, opts))
      case _ => None
    }
  }

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
   * Computes the signature using the given [[LogicalRelation]]. This computes a signature of
   * using all the files found in [[PartitioningAwareFileIndex]].
   *
   * @param logicalPlan Logical plan to compute signature from.
   * @return Signature computed if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def signature(logicalPlan: LogicalPlan): Option[String] = {
    getLogicalRelation(logicalPlan).map(_.relation).flatMap {
      case HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, format, _)
          if isSupportedFileFormat(format) =>
        val result = filesFromIndex(location).sortBy(_.getPath.toString).foldLeft("") {
          (acc: String, f: FileStatus) =>
            HashingUtils.md5Hex(acc + fingerprint(f))
        }
        Some(result)
      case _ => None
    }
  }

  /**
   * Fingerprints a file.
   *
   * @param fileStatus File status.
   * @return The fingerprint of a file.
   */
  private def fingerprint(fileStatus: FileStatus): String = {
    fileStatus.getLen.toString + fileStatus.getModificationTime.toString +
      fileStatus.getPath.toString
  }

  /**
   * Retrieves all input files from the given [[LogicalRelation]].
   *
   * @param logicalPlan Logical plan to retrieve input files from.
   * @return List of [[FileStatus]] for the given relation.
   */
  override def allFiles(logicalPlan: LogicalPlan): Option[Seq[FileStatus]] = {
    getLogicalRelation(logicalPlan).map(_.relation).flatMap {
      case HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _) =>
        Some(filesFromIndex(location))
      case _ => None
    }
  }

  /**
   * Constructs the basePath for the given [[LogicalPlan]].
   *
   * @param logicalPlan Logical plan to extract the base path from.
   * @return basePath to read the given partitioned location.
   *         Some(Some(path)) => The location of the given plan is supported
   *                             and partition is specified.
   *         Some(None) => The location of the given plan is supported
   *                       but is un-partitioned.
   *         None => The location of the given plan is not supported.
   */
  override def partitionBasePath(logicalPlan: LogicalPlan): Option[Option[String]] = {
    getLogicalRelation(logicalPlan).map(_.relation).flatMap {
      case HadoopFsRelation(location, _, _, _, _, _) =>
        location match {
          case p: PartitioningAwareFileIndex if p.partitionSpec.partitions.nonEmpty =>
            // For example, we could have the following in PartitionSpec:
            //   - partition columns = "col1", "col2"
            //   - partitions: "/path/col1=1/col2=1", "/path/col1=1/col2=2", etc.
            // , and going up the same number of directory levels as the number of partition columns
            // will compute the base path. Note that PartitionSpec.partitions will always contain
            // all the partitions in the path, so "partitions.head" is taken as an initial value.
            val basePath = p.partitionSpec.partitionColumns
                .foldLeft(p.partitionSpec.partitions.head.path)((path, _) => path.getParent)
            Some(Some(basePath.toString))
          case _: PartitioningAwareFileIndex => Some(None)
          case _ => None
        }
    }
  }

  /**
   * Returns list of pairs of (file path, file id) to build lineage column.
   *
   * File paths should be the same format as "input_file_name()" of the given relation type.
   * For [[DefaultFileBasedSource]], each file path should be in this format:
   *   `file:///path/to/file`
   *
   * @param logicalPlan Logical plan to check the relation type.
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of pairs of (file path, file id).
   */
  override def lineagePairs(logicalPlan: LogicalPlan,
      fileIdTracker: FileIdTracker): Option[Seq[(String, Long)]] = {
    getLogicalRelation(logicalPlan).map(_.relation).flatMap {
      case HadoopFsRelation(_: PartitioningAwareFileIndex, _, _, _, format, _)
          if isSupportedFileFormat(format) =>
        Some(fileIdTracker.getFileToIdMap.toSeq.map { kv =>
          (kv._1._1.replace("file:/", "file:///"), kv._2)
        })
      case _ => None
    }
  }

  /**
   * Returns whether the given relation has parquet source files or not.
   *
   * @param logicalPlan Logical plan to check the source file format.
   * @return True if source files in the given relation are parquet.
   */
  override def hasParquetAsSourceFormat(logicalPlan: LogicalPlan): Option[Boolean] = {
    getLogicalRelation(logicalPlan).map(_.relation).flatMap {
      case HadoopFsRelation(_: PartitioningAwareFileIndex, _, _, _, format, _)
          if isSupportedFileFormat(format) =>
        val fileFormatName = format.asInstanceOf[DataSourceRegister].shortName
        Some(fileFormatName.equals("parquet"))
      case _ => None
    }
  }

  private def filesFromIndex(index: PartitioningAwareFileIndex): Seq[FileStatus] =
    try {
      // Keep the `asInstanceOf` to force casting or fallback because Databrick's
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
 * Builder for building [[DefaultFileBasedSource]].
 */
class DefaultFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new DefaultFileBasedSource(spark)
}
