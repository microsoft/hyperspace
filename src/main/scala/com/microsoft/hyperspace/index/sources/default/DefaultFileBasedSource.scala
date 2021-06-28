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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.DataSourceRegister

import com.microsoft.hyperspace.index.Relation
import com.microsoft.hyperspace.index.sources.{FileBasedRelation, FileBasedRelationMetadata, FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}
import com.microsoft.hyperspace.util.{CacheWithTransform, HyperspaceConf}

/**
 * Default implementation for file-based Spark built-in sources such as parquet, csv, json, etc.
 *
 * This source can support relations that meet the following criteria:
 *   - The relation is [[HadoopFsRelation]] with [[PartitioningAwareFileIndex]] as file index.
 *   - Its file format implements [[DataSourceRegister]].
 */
class DefaultFileBasedSource(private val spark: SparkSession) extends FileBasedSourceProvider {
  private val supportedFormats: CacheWithTransform[String, Set[String]] =
    new CacheWithTransform[String, Set[String]](
      { () =>
        HyperspaceConf.supportedFileFormatsForDefaultFileBasedSource(spark)
      },
      { formats =>
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
   * Returns true if the given logical plan is a supported relation.
   *
   * @param plan Logical plan to check if it's supported.
   * @return Some(true) if the given plan is a supported relation, otherwise None.
   */
  def isSupportedRelation(plan: LogicalPlan): Option[Boolean] =
    plan match {
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
  def getRelation(plan: LogicalPlan): Option[FileBasedRelation] = {
    if (isSupportedRelation(plan).contains(true)) {
      Some(new DefaultFileBasedRelation(spark, plan.asInstanceOf[LogicalRelation]))
    } else {
      None
    }
  }

  override def isSupportedRelationMetadata(metadata: Relation): Option[Boolean] = {
    if (isSupportedFileFormatName(metadata.fileFormat)) {
      Some(true)
    } else {
      None
    }
  }

  override def getRelationMetadata(metadata: Relation): Option[FileBasedRelationMetadata] = {
    if (isSupportedRelationMetadata(metadata).contains(true)) {
      Some(new DefaultFileBasedRelationMetadata(metadata))
    } else {
      None
    }
  }
}

/**
 * Builder for building [[DefaultFileBasedSource]].
 */
class DefaultFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new DefaultFileBasedSource(spark)
}
