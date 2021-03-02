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

package com.microsoft.hyperspace.index.sources

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index.{FileIdTracker, FileInfo, IndexLogEntry, Relation}

/**
 * ::Experimental::
 * A trait that represents a relation for a source provider.
 *
 * @since 0.5.0
 */
trait SourceRelation

/**
 * ::Experimental::
 * A trait that a source provider should implement to represent the source relation
 * that is based on files.
 *
 * @since 0.5.0
 */
trait FileBasedRelation extends SourceRelation {

  /**
   * The logical plan that this FileBasedRelation wraps.
   */
  def plan: LogicalPlan

  /**
   * Options of the current relation.
   */
  def options: Map[String, String]

  /**
   * Computes the signature of the current relation.
   *
   * This API is used when the signature of source needs to be computed, e.g., creating an index,
   * computing query plan's signature, etc.
   */
  def signature: String

  /**
   * FileStatus list for all source files that the current relation references to.
   */
  val allFiles: Seq[FileStatus]

  /**
   * FileInfo list for all source files that the current relation references to.
   */
  val allFileInfos: Seq[FileInfo]

  /**
   * Summation of all source file size.
   */
  val allFileSizeInBytes: Long

  /**
   * The partition schema of the current relation.
   */
  def partitionSchema: StructType

  /**
   * The optional partition base path of the current relation.
   */
  def partitionBasePath: Option[String]

  /**
   * Creates [[HadoopFsRelation]] based on the current relation.
   *
   * This is mainly used in conjunction with [[createLogicalRelation]].
   */
  def createHadoopFsRelation(
      location: FileIndex,
      dataSchema: StructType,
      options: Map[String, String]): HadoopFsRelation

  /**
   * Creates [[LogicalRelation]] based on the current relation.
   *
   * This is mainly used to read the index files.
   */
  def createLogicalRelation(
      hadoopFsRelation: HadoopFsRelation,
      newOutput: Seq[AttributeReference]): LogicalRelation

  // TODO: APIs defined are below are related to index maintenance.
  //   This can be moved out to a separate trait.

  /**
   * Creates [[Relation]] for IndexLogEntry using the current relation.
   */
  def createRelationMetadata(fileIdTracker: FileIdTracker): Relation

  /**
   * Returns whether the current relation has parquet source files or not.
   *
   * @return True if source files of the current relation are parquet.
   */
  def hasParquetAsSourceFormat: Boolean

  /**
   * Returns list of pairs of (file path, file id) to build lineage column.
   */
  def lineagePairs(fileIdTracker: FileIdTracker): Seq[(String, Long)]

  /**
   * Returns IndexLogEntry of the closest index version for the given relation.
   *
   * curFiles is used to calculate the similarity with each index version data.
   *
   * @param index Candidate index to be applied.
   * @return IndexLogEntry of the closest version among available index versions.
   */
  def closestIndexVersion(index: IndexLogEntry): IndexLogEntry = {
    index
  }
}

/**
 * ::Experimental::
 * A trait that a data source should implement so that an index can be created/managed and
 * utilized for the data source.
 *
 * @since 0.4.0
 */
trait SourceProvider

/**
 * ::Experimental::
 * A trait that a source provider's builder should implement. Each source provider should have an
 * accompanying builder in order to be plugged into the SourceProviderManager.
 *
 * The reason for having a builder is to inject [[SparkSession]] to the source provider if needed.
 *
 * @since 0.4.0
 */
trait SourceProviderBuilder {

  /**
   * Builds a [[SourceProvider]].
   *
   * @param spark Spark session.
   * @return [[SourceProvider]] object.
   */
  def build(spark: SparkSession): SourceProvider
}

/**
 * ::Experimental::
 * A trait that a data source should implement so that an index can be created/managed and
 * utilized for the data source.
 *
 * @since 0.4.0
 */
trait FileBasedSourceProvider extends SourceProvider {

  /**
   * Given a [[Relation]], returns a new [[Relation]] that will have the latest source.
   *
   * This API is used when an index is refreshed.
   *
   * If the given relation does not belong to this provider, None should be returned.
   *
   * @param relation [[Relation]] object to reconstruct [[DataFrame]] with.
   * @return [[Relation]] object if the given 'relation' can be processed by this provider.
   *         Otherwise, None.
   */
  def refreshRelationMetadata(relation: Relation): Option[Relation]

  /**
   * Returns a file format name to read internal data for a given [[Relation]].
   *
   * @param relation [[Relation]] object to read internal data files.
   * @return File format to read internal data files.
   */
  def internalFileFormatName(relation: Relation): Option[String]

  /**
   * Returns true if the given logical plan is a supported relation.
   *
   * @param plan Logical plan to check if it's supported.
   * @return Some(true) if the given plan is a supported relation, otherwise None.
   */
  def isSupportedRelation(plan: LogicalPlan): Option[Boolean]

  /**
   * Returns the [[FileBasedRelation]] that wraps the given logical plan if the given
   * logical plan is a supported relation.
   *
   * @param plan Logical plan to wrap to [[FileBasedRelation]]
   * @return [[FileBasedRelation]] that wraps the given logical plan.
   */
  def getRelation(plan: LogicalPlan): Option[FileBasedRelation]

  /**
   * Returns enriched index properties.
   *
   * @param relation Logical relation to retrieve necessary information.
   * @param previousProperties Index properties of previous index version.
   * @return Updated index properties for index creation or refreshment.
   */
  def enrichIndexProperties(
      relation: Relation,
      previousProperties: Map[String, String]): Option[Map[String, String]]
}
