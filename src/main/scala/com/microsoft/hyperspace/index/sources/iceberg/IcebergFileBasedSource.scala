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

package com.microsoft.hyperspace.index.sources.iceberg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import com.microsoft.hyperspace.index.Relation
import com.microsoft.hyperspace.index.sources.{FileBasedRelation, FileBasedRelationMetadata, FileBasedSourceProvider, SourceProvider, SourceProviderBuilder}

/**
 * Iceberg file-based source provider.
 *
 * This source can support relations that meet the following criteria:
 *   - The relation is with [[DataSourceV2Relation]] or [[DataSourceV2ScanRelation]]
 *   - The source of [[DataSourceV2Relation]] is an [[IcebergSource]],
 *     or the table is an Iceberg table.
 */
class IcebergFileBasedSource(private val spark: SparkSession) extends FileBasedSourceProvider {

  private val ICEBERG_FORMAT_STR = "iceberg"

  /**
   * Returns true if the given logical plan is a relation for Iceberg.
   *
   * @param plan Logical plan to check if it's supported.
   * @return Some(true) if the given plan is a supported relation, otherwise None.
   */
  override def isSupportedRelation(plan: LogicalPlan): Option[Boolean] = {
    Some(IcebergShims.isIcebergRelation(plan)).filter(_ == true)
  }

  /**
   * Returns the [[FileBasedRelation]] that wraps the given logical plan if the given
   * logical plan is a supported relation.
   *
   * @param plan Logical plan to wrap to [[FileBasedRelation]]
   * @return [[FileBasedRelation]] that wraps the given logical plan.
   */
  override def getRelation(plan: LogicalPlan): Option[FileBasedRelation] = {
    if (isSupportedRelation(plan).contains(true)) {
      val (table, snapshotId) = IcebergShims.loadIcebergTable(spark, plan)
      Some(new IcebergRelation(spark, table, snapshotId, plan))
    } else {
      None
    }
  }

  override def isSupportedRelationMetadata(metadata: Relation): Option[Boolean] = {
    if (metadata.fileFormat.equals(ICEBERG_FORMAT_STR)) {
      Some(true)
    } else {
      None
    }
  }

  override def getRelationMetadata(metadata: Relation): Option[FileBasedRelationMetadata] = {
    if (isSupportedRelationMetadata(metadata).contains(true)) {
      Some(new IcebergRelationMetadata(metadata))
    } else {
      None
    }
  }
}

/**
 * Builder for building [[IcebergFileBasedSource]].
 */
class IcebergFileBasedSourceBuilder extends SourceProviderBuilder {
  override def build(spark: SparkSession): SourceProvider = new IcebergFileBasedSource(spark)
}
