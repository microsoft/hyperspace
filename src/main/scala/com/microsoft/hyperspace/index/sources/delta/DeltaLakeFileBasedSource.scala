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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.index.{IndexConstants, Relation}
import com.microsoft.hyperspace.index.sources.{FileBasedRelation, FileBasedRelationMetadata, FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}

object DeltaLakeConstants {
  val DELTA_FORMAT_STR = "delta"

  // JSON property name used in index metadata to store Delta Lake version history of refresh.
  val DELTA_VERSION_HISTORY_PROPERTY = "deltaVersions"
}

/**
 * Delta Lake file-based source provider.
 *
 * This source can support relations that meet the following criteria:
 *   - The relation is [[HadoopFsRelation]] with [[TahoeLogFileIndex]] as file index.
 */
class DeltaLakeFileBasedSource(private val spark: SparkSession) extends FileBasedSourceProvider {

  /**
   * Returns true if the given logical plan is a relation for Delta Lake.
   *
   * @param plan Logical plan to check if it's supported.
   * @return Some(true) if the given plan is a supported relation, otherwise None.
   */
  def isSupportedRelation(plan: LogicalPlan): Option[Boolean] =
    plan match {
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
  def getRelation(plan: LogicalPlan): Option[FileBasedRelation] = {
    if (isSupportedRelation(plan).contains(true)) {
      Some(new DeltaLakeRelation(spark, plan.asInstanceOf[LogicalRelation]))
    } else {
      None
    }
  }

  override def isSupportedRelationMetadata(metadata: Relation): Option[Boolean] = {
    if (metadata.fileFormat.equals(DeltaLakeConstants.DELTA_FORMAT_STR)) {
      Some(true)
    } else {
      None
    }
  }

  override def getRelationMetadata(metadata: Relation): Option[FileBasedRelationMetadata] = {
    if (isSupportedRelationMetadata(metadata).contains(true)) {
      Some(new DeltaLakeRelationMetadata(metadata))
    } else {
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
