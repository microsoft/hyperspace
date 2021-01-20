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

package com.microsoft.hyperspace.index

import java.util.Locale

/**
 * IndexConfig specifies the configuration of an index.
 *
 * @param indexName Index name.
 * @param indexedColumns Columns from which an index is created.
 * @param includedColumns Columns to be included in the index.
 */
case class IndexConfig(
    indexName: String,
    indexedColumns: Seq[String],
    includedColumns: IncludedColumnsConfig = IncludedColumnsConfig()) {
  if (indexName.isEmpty || indexedColumns.isEmpty) {
    throw new IllegalArgumentException("Empty index name or indexed columns are not allowed.")
  }

  val lowerCaseIndexedColumns = toLowerCase(indexedColumns)
  val lowerCaseIncludedColumnsIncludeSet = includedColumns.lowerCaseIncludeColumnsSet
  val lowerCaseIncludedColumnsExcludeSet = includedColumns.lowerCaseExcludeColumnsSet

  if (lowerCaseIndexedColumns.toSet.size < lowerCaseIndexedColumns.size) {
    throw new IllegalArgumentException("Duplicate indexed column names are not allowed.")
  }

  if (lowerCaseIncludedColumnsIncludeSet.size < includedColumns.lowerCaseIncludeColumns.size ||
      lowerCaseIncludedColumnsExcludeSet.size < includedColumns.lowerCaseExcludeColumns.size) {
    throw new IllegalArgumentException(
      "Duplicate include or exclude column names in included columns are not allowed.")
  }

  if (lowerCaseIncludedColumnsIncludeSet.nonEmpty && lowerCaseIncludedColumnsExcludeSet.nonEmpty) {
    throw new IllegalArgumentException(
      "IncludedColumnsConfig include and exclude columns can not both be non-empty.")
  }

  if (lowerCaseIncludedColumnsIncludeSet.intersect(lowerCaseIncludedColumnsExcludeSet).nonEmpty) {
    throw new IllegalArgumentException(
      "IncludedColumnsConfig include and exclude columns can not overlap.")
  }

  for (indexedColumn <- lowerCaseIndexedColumns) {
    if (lowerCaseIncludedColumnsIncludeSet.contains(indexedColumn) ||
        lowerCaseIncludedColumnsExcludeSet.contains(indexedColumn)) {
      throw new IllegalArgumentException(
        "Duplicate column names in indexed/included columns are not allowed.")
    }
  }

  override def equals(that: Any): Boolean = {
    that match {
      case IndexConfig(thatIndexName, thatIndexedColumns, thatIncludedColumns) =>
        indexName.equalsIgnoreCase(thatIndexName) &&
          lowerCaseIndexedColumns.equals(toLowerCase(thatIndexedColumns)) &&
          includedColumns.equals(thatIncludedColumns)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    lowerCaseIndexedColumns.hashCode + includedColumns.hashCode()
  }

  override def toString: String = {
    val indexedColumnNames = lowerCaseIndexedColumns.mkString(", ")
    val includedColumnsString = includedColumns.toString
    s"[indexName: $indexName; indexedColumns: $indexedColumnNames; " +
      s"includedColumns: $includedColumnsString]"
  }

  private def toLowerCase(seq: Seq[String]): Seq[String] = seq.map(_.toLowerCase(Locale.ROOT))
}

/**
 * Defines [[IndexConfig.Builder]] and relevant helper methods for enabling builder pattern for
 * [[IndexConfig]].
 */
object IndexConfig {

  def apply(indexName: String, indexedColumns: Seq[String]): IndexConfig = {
    IndexConfig(indexName, indexedColumns, IncludedColumnsConfig())
  }

  def apply(
      indexName: String,
      indexedColumns: Seq[String],
      columnsToInclude: Seq[String]): IndexConfig = {
    IndexConfig(indexName, indexedColumns, IncludedColumnsConfig(columnsToInclude))
  }

  /**
   * Builder for [[IndexConfig]].
   */
  class Builder {

    private[this] var indexedColumns: Seq[String] = Seq()
    private[this] var columnsToInclude: Seq[String] = Seq()
    private[this] var columnsToExclude: Seq[String] = Seq()
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
     * Updates included columns for [[IndexConfig.includedColumns]].
     *
     * Note: API signature supports passing one or more argument.
     *
     * @param includedColumn included column for [[IndexConfig.includedColumns]].
     * @param includedColumns included columns for [[IndexConfig.includedColumns]].
     * @return an [[IndexConfig.Builder]] object with updated included columns.
     */
    def include(includedColumn: String, includedColumns: String*): Builder = {
      if (this.columnsToInclude.nonEmpty) {
        throw new UnsupportedOperationException("Included columns are already set.")
      }

      this.columnsToInclude = includedColumn +: includedColumns
      this
    }

    /**
     * Updates excluded columns for [[IndexConfig.includedColumns]].
     *
     * Note: API signature supports passing one or more argument.
     *
     * @param excludedColumn excluded column for [[IndexConfig.includedColumns]].
     * @param excludedColumns excluded columns for [[IndexConfig.includedColumns]].
     * @return an [[IndexConfig.Builder]] object with updated excluded columns.
     */
    def exclude(excludedColumn: String, excludedColumns: String*): Builder = {
      if (this.columnsToExclude.nonEmpty) {
        throw new UnsupportedOperationException("Excluded columns are already set.")
      }

      this.columnsToExclude = excludedColumn +: excludedColumns
      this
    }

    /**
     * Creates IndexConfig from supplied index name, indexed columns and included columns
     * to [[IndexConfig.Builder]].
     *
     * @return an [[IndexConfig]] object.
     */
    def create(): IndexConfig = {
      IndexConfig(
        indexName,
        indexedColumns,
        IncludedColumnsConfig(columnsToInclude, columnsToExclude))
    }
  }

  /**
   * Creates new [[IndexConfig.Builder]] for constructing an [[IndexConfig]].
   *
   * @return an [[IndexConfig.Builder]] object.
   */
  def builder(): Builder = new Builder
}

/**
 * Specifies list of included columns in terms of columns to include and/or exclude.
 *
 * @param include List of column names to include as include columns.
 * @param exclude List of column names to exclude to form list of included columns.
 */
case class IncludedColumnsConfig(include: Seq[String] = Nil, exclude: Seq[String] = Nil) {
  lazy val lowerCaseIncludeColumns = toLowerCase(include)
  lazy val lowerCaseExcludeColumns = toLowerCase(exclude)
  lazy val lowerCaseIncludeColumnsSet = lowerCaseIncludeColumns.toSet
  lazy val lowerCaseExcludeColumnsSet = lowerCaseExcludeColumns.toSet

  override def equals(that: Any): Boolean = {
    that match {
      case IncludedColumnsConfig(thatInclude, thatExclude) =>
        lowerCaseIncludeColumnsSet.equals(toLowerCase(thatInclude).toSet) &&
          lowerCaseExcludeColumnsSet.equals(toLowerCase(thatExclude).toSet)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    lowerCaseIncludeColumnsSet.hashCode + lowerCaseExcludeColumnsSet.hashCode
  }

  override def toString: String = {
    val includeColumnNames = lowerCaseIncludeColumns.mkString(", ")
    val excludeColumnNames = lowerCaseExcludeColumns.mkString(", ")
    s"[include: $includeColumnNames; exclude: $excludeColumnNames]"
  }

  private def toLowerCase(seq: Seq[String]): Seq[String] = seq.map(_.toLowerCase(Locale.ROOT))
}
