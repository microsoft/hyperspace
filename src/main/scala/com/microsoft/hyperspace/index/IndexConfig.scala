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

import java.util.Locale

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * IndexConfig specifies the configuration of an index.
 *
 * Implementation note: the name of this class might be changed in v1.0.
 *
 * @param indexName Index name.
 * @param indexedColumns Columns from which an index is created.
 * @param includedColumns Columns to be included in the index.
 */
case class IndexConfig(
    override val indexName: String,
    indexedColumns: Seq[String],
    includedColumns: Seq[String] = Seq())
    extends IndexConfigTrait {
  if (indexName.isEmpty || indexedColumns.isEmpty) {
    throw new IllegalArgumentException("Empty index name or indexed columns are not allowed.")
  }

  val lowerCaseIndexedColumns = toLowerCase(indexedColumns)
  val lowerCaseIncludedColumns = toLowerCase(includedColumns)
  val lowerCaseIncludedColumnsSet = lowerCaseIncludedColumns.toSet

  if (lowerCaseIndexedColumns.toSet.size < lowerCaseIndexedColumns.size) {
    throw new IllegalArgumentException("Duplicate indexed column names are not allowed.")
  }

  if (lowerCaseIncludedColumnsSet.size < lowerCaseIncludedColumns.size) {
    throw new IllegalArgumentException("Duplicate included column names are not allowed.")
  }

  for (indexedColumn <- lowerCaseIndexedColumns) {
    if (lowerCaseIncludedColumns.contains(indexedColumn)) {
      throw new IllegalArgumentException(
        "Duplicate column names in indexed/included columns are not allowed.")
    }
  }

  override def equals(that: Any): Boolean = {
    that match {
      case IndexConfig(thatIndexName, thatIndexedColumns, thatIncludedColumns) =>
        indexName.equalsIgnoreCase(thatIndexName) &&
          lowerCaseIndexedColumns.equals(toLowerCase(thatIndexedColumns)) &&
          lowerCaseIncludedColumnsSet.equals(toLowerCase(thatIncludedColumns).toSet)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    lowerCaseIndexedColumns.hashCode + lowerCaseIncludedColumnsSet.hashCode
  }

  override def toString: String = {
    val indexedColumnNames = lowerCaseIndexedColumns.mkString(", ")
    val includedColumnNames = lowerCaseIncludedColumns.mkString(", ")
    s"[indexName: $indexName; indexedColumns: $indexedColumnNames; " +
      s"includedColumns: $includedColumnNames]"
  }

  private def toLowerCase(seq: Seq[String]): Seq[String] = seq.map(_.toLowerCase(Locale.ROOT))

  override def referencedColumns: Seq[String] = indexedColumns ++ includedColumns

  override def createIndex(
      ctx: IndexerContext,
      sourceData: DataFrame,
      properties: Map[String, String]): (Index, DataFrame) = {
    val (indexData, resolvedIndexedColumns, resolvedIncludedColumns) =
      CoveringIndex.createIndexData(
        ctx,
        sourceData,
        indexedColumns,
        includedColumns,
        CoveringIndex.hasLineageColumn(properties))
    val index = CoveringIndex(
      resolvedIndexedColumns.map(_.normalizedName),
      resolvedIncludedColumns.map(_.normalizedName),
      indexData.schema,
      HyperspaceConf.numBucketsForIndex(ctx.spark),
      properties)
    (index, indexData)
  }
}

/**
 * Defines [[IndexConfig.Builder]] and relevant helper methods for enabling builder pattern for
 * [[IndexConfig]].
 */
object IndexConfig {

  /**
   * Builder for [[IndexConfig]].
   */
  class Builder {

    private[this] var indexedColumns: Seq[String] = Seq()
    private[this] var includedColumns: Seq[String] = Seq()
    private[this] var indexName: String = ""

    /**
     * Updates index name for [[IndexConfig]].
     *
     * @param indexName index name for the [[IndexConfig]].
     * @return an [[IndexConfig.Builder]] object with updated index name.
     */
    def indexName(indexName: String): Builder = {
      if (!this.indexName.isEmpty) {
        throw new UnsupportedOperationException("Index name is already set.")
      }

      if (indexName.isEmpty) {
        throw new IllegalArgumentException("Empty index name is not allowed.")
      }

      this.indexName = indexName
      this
    }

    /**
     * Updates column names for [[IndexConfig]].
     *
     * Note: API signature supports passing one or more argument.
     *
     * @param indexedColumn indexed column for the [[IndexConfig]].
     * @param indexedColumns indexed columns for the [[IndexConfig]].
     * @return an [[IndexConfig.Builder]] object with updated indexed columns.
     */
    def indexBy(indexedColumn: String, indexedColumns: String*): Builder = {
      if (this.indexedColumns.nonEmpty) {
        throw new UnsupportedOperationException("Indexed columns are already set.")
      }

      this.indexedColumns = indexedColumn +: indexedColumns
      this
    }

    /**
     * Updates included columns for [[IndexConfig]].
     *
     * Note: API signature supports passing one or more argument.
     *
     * @param includedColumn included column for [[IndexConfig]].
     * @param includedColumns included columns for [[IndexConfig]].
     * @return an [[IndexConfig.Builder]] object with updated included columns.
     */
    def include(includedColumn: String, includedColumns: String*): Builder = {
      if (this.includedColumns.nonEmpty) {
        throw new UnsupportedOperationException("Included columns are already set.")
      }

      this.includedColumns = includedColumn +: includedColumns
      this
    }

    /**
     * Creates IndexConfig from supplied index name, indexed columns and included columns
     * to [[IndexConfig.Builder]].
     *
     * @return an [[IndexConfig]] object.
     */
    def create(): IndexConfig = {
      IndexConfig(indexName, indexedColumns, includedColumns)
    }
  }

  /**
   *  Creates new [[IndexConfig.Builder]] for constructing an [[IndexConfig]].
   *
   * @return an [[IndexConfig.Builder]] object.
   */
  def builder(): Builder = new Builder
}
