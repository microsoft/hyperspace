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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.DataSourceRegister

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, Relation}
import com.microsoft.hyperspace.index.IndexConstants.GLOBBING_PATTERN_KEY
import com.microsoft.hyperspace.index.sources.{FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}
import com.microsoft.hyperspace.util.{CacheWithTransform, HashingUtils, HyperspaceConf, PathUtils}

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
   * Creates [[Relation]] for IndexLogEntry using the given [[LogicalRelation]].
   *
   * @param logicalRelation logical relation to derive [[Relation]] from.
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def createRelation(
      logicalRelation: LogicalRelation,
      fileIdTracker: FileIdTracker): Option[Relation] = {
    logicalRelation.relation match {
      case HadoopFsRelation(
          location: PartitioningAwareFileIndex,
          _,
          dataSchema,
          _,
          fileFormat,
          options) if isSupportedFileFormat(fileFormat) =>
        val files = location.allFiles
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties =
          Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
        val fileFormatName = fileFormat.asInstanceOf[DataSourceRegister].shortName

        // Store basePath of hive-partitioned data sources, if applicable.
        val basePath = options.get("basePath") match {
          case None => PathUtils.extractBasePath(location.partitionSpec())
          case p => p
        }

        // "path" key in options can incur multiple data read unexpectedly.
        // Since "options" is case-insensitive map, it will change any previous entries of
        // "basePath" to lowercase "basepath", making it unusable.
        // Remove lowercase "basepath" and add proper cased "basePath".
        val opts = basePath match {
          case Some(path) => Map("basePath" -> path) ++ options - "path" - "basepath"
          case _ => options - "path" - "basepath"
        }

        val rootPaths = opts.get(GLOBBING_PATTERN_KEY) match {
          case Some(pattern) =>
            // Validate if globbing pattern matches actual source paths.
            // This logic is picked from the globbing logic at:
            // https://github.com/apache/spark/blob/v2.4.4/sql/core/src/main/scala/org/apache/
            // spark/sql/execution/datasources/DataSource.scala#L540
            val fs = location.allFiles.head.getPath.getFileSystem(new Configuration)
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
   * Computes the signature using the given [[LogicalRelation]]. This computes a signature of
   * using all the files found in [[PartitioningAwareFileIndex]].
   *
   * @param logicalRelation logical relation to compute signature from.
   * @return Signature computed if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  override def signature(logicalRelation: LogicalRelation): Option[String] = {
    logicalRelation.relation match {
      case HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, format, _)
          if isSupportedFileFormat(format) =>
        val result = location.allFiles.sortBy(_.getPath.toString).foldLeft("") {
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
   * @param fileStatus file status.
   * @return the fingerprint of a file.
   */
  private def fingerprint(fileStatus: FileStatus): String = {
    fileStatus.getLen.toString + fileStatus.getModificationTime.toString +
      fileStatus.getPath.toString
  }
}

/**
 * Builder for building [[DefaultFileBasedSource]].
 */
class DefaultFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new DefaultFileBasedSource(spark)
}
