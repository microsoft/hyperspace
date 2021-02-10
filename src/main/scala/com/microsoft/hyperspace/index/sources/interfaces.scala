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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.{FileIdTracker, Relation}

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
   * Creates [[Relation]] for IndexLogEntry using the given [[LogicalPlan]].
   *
   * This API is used when an index is created.
   *
   * If the given logical relation does not belong to this provider, None should be returned.
   *
   * @param logicalPlan Logical relation to derive [[Relation]] from.
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  def createRelation(
      logicalPlan: LogicalPlan,
      fileIdTracker: FileIdTracker): Option[Relation]

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
  def refreshRelation(relation: Relation): Option[Relation]

  /**
   * Returns a file format name to read internal data for a given [[Relation]].
   *
   * @param relation [[Relation]] object to read internal data files.
   * @return File format to read internal data files.
   */
  def internalFileFormatName(relation: Relation): Option[String]

  /**
   * Computes the signature using the given [[LogicalRelation]].
   *
   * This API is used when the signature of source needs to be computed, e.g., creating an index,
   * computing query plan's signature, etc.
   *
   * If the given logical relation does not belong to this provider, None should be returned.
   *
   * @param logicalPlan Logical relation to compute signature from.
   * @return Signature computed if the given 'logicalRelation' can be processed by this provider.
   *         Otherwise, None.
   */
  def signature(logicalPlan: LogicalPlan): Option[String]

  /**
   * Retrieves all input files from the given [[LogicalPlan]].
   *
   * @param logicalPlan Logical relation to retrieve input files from.
   * @return List of [[FileStatus]] for the given relation.
   */
  def allFiles(logicalPlan: LogicalPlan): Option[Seq[FileStatus]]

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
  def partitionBasePath(logicalPlan: LogicalPlan): Option[Option[String]]

  /**
   * Returns list of pairs of (file path, file id) to build lineage column.
   *
   * File paths should be the same format with "input_file_name()" of the given relation type.
   *
   * @param logicalPlan Logical plan to check the relation type.
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of pairs of (file path, file id).
   */
  def lineagePairs(
      logicalPlan: LogicalPlan,
      fileIdTracker: FileIdTracker): Option[Seq[(String, Long)]]

  /**
   * Returns whether the given relation has parquet source files or not.
   *
   * @param logicalPlan Logical plan to check the source file format.
   * @return True if source files in the given relation are parquet.
   */
  def hasParquetAsSourceFormat(logicalPlan: LogicalPlan): Option[Boolean]

}
