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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.index.Relation
import com.microsoft.hyperspace.index.sources.{FileBasedRelation, FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}

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
  override def refreshRelationMetadata(relation: Relation): Option[Relation] = {
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
