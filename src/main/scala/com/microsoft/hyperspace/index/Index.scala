/*
 * Copyright (2021) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.index

import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame

/**
 * Represents an index.
 *
 * Index is any data structure that can be used to improve the performance
 * of a query that references source data indexed by the index by rewriting
 * the query and returning a new, more efficient plan.
 *
 * The framework manages various types of indexes through this interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
trait Index {

  /**
   * Returns the name of this index type.
   */
  def kind: String

  /**
   * Returns the abbreviated name of this index type, which may be used instead
   * of the full name when screen space is limited.
   */
  def kindAbbr: String

  /**
   * Returns the column names to be indexed.
   */
  def indexedColumns: Seq[String]

  /**
   * Returns the column names this index references.
   *
   * Referenced columns include indexed columns, plus additional columns
   * that the index may reference.
   */
  def referencedColumns: Seq[String]

  /**
   * Returns a string-valued key-value map for this index.
   *
   * This is used by the framework and source providers to store some
   * information about the index. Index implementations can also utilize this,
   * but defining properties directly on the class is a recommended way. When
   * using this map to store implementation-specific values, use an appropriate
   * naming scheme for keys to avoid collisions with names used by the framework
   * and source providers.
   */
  def properties: Map[String, String]

  /**
   * Returns a copy of this index, with the properties replaced with
   * the new properties.
   */
  def withNewProperties(newProperties: Map[String, String]): Index

  /**
   * Returns information about the index.
   *
   * @param extended If true, returns more information.
   * @return Implementation-specific key-value map describing the index. It
   *         should not include information that is available in the interface,
   *         such as indexed columns.
   */
  def statistics(extended: Boolean = false): Map[String, String]

  /**
   * Returns true if this index can be applied to a relation for which some
   * files are deleted since the creation of the index.
   */
  def canHandleDeletedFiles: Boolean

  /**
   * Writes the index data to the specified path.
   *
   * Any data that was already at the specified path is overwritten.
   *
   * @param ctx Helper object for indexing operations
   * @param indexData Index data to write
   */
  def write(ctx: IndexerContext, indexData: DataFrame): Unit

  /**
   * Optimizes index data files. The framework determines files to optimize
   * by considering the optimization mode (currently, "quick" or "full") and
   * the file size threshold for optimization, and those files are passed to
   * this method.
   *
   * @param ctx Helper object for indexing operations
   * @param indexDataFilesToOptimize Index data files to optimize
   */
  def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit

  /**
   * Incrementally refreshes the index data to account for appended and deleted
   * source data files.
   *
   * @param ctx Helper object for indexing operations
   * @param appendedSourceData Source dataframe for appended files, if there are
   *   appended files; None otherwise
   * @param deletedSourceDataFiles Source data files deleted from the source
   *   since the creation of this index
   * @param indexContent Unrefreshed index data files
   * @return Pair of (updated index, update mode) where the first value is an
   *         updated index resulting from the indexing operation or this index
   *         if no update is needed, and the second value is whether the
   *         updated index data needs to be merged to the existing index data
   *         or the existing index data should be overwritten
   */
  def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (Index, Index.UpdateMode)

  /**
   * Indexes the source data and returns an updated index and index data.
   *
   * This is for a full rebuild of the index. It should return an updated index
   * if it is necessary for the index implementation. Otherwise, it can simply
   * return this index.
   *
   * @param ctx Helper object for indexing operations
   * @param sourceData Source data to index
   * @return Updated index resulting from the indexing operation, or this
   *         index if no update is needed
   */
  def refreshFull(ctx: IndexerContext, sourceData: DataFrame): (Index, DataFrame)

  /**
   * Returns true if and only if this index equals to that.
   *
   * Indexes only differing in their [[properties]] should be considered equal.
   */
  def equals(that: Any): Boolean

  /**
   * Returns the hash code for this index.
   */
  def hashCode: Int
}

object Index {
  sealed trait UpdateMode
  object UpdateMode {
    case object Merge extends UpdateMode
    case object Overwrite extends UpdateMode
  }
}
