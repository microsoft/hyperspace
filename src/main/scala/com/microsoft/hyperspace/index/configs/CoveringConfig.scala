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

package com.microsoft.hyperspace.index.configs

import com.microsoft.hyperspace.index.IndexConfig

/**
 * Defines [[CoveringConfig.Builder]] and relevant helper methods for enabling builder
 * pattern for [[CoveringConfig]].
 */
object CoveringConfig {

  /**
   * Builder for [[CoveringConfig]].
   */
  class Builder {

    private[this] var indexedColumns: Seq[String] = Seq()
    private[this] var includedColumns: Seq[String] = Seq()
    private[this] var indexName: String = ""

    /**
     * Updates index name for [[CoveringConfig]].
     *
     * @param indexName index name for the [[CoveringConfig]].
     * @return an [[CoveringConfig.Builder]] object with updated index name.
     */
    def indexName(indexName: String): Builder = {
      if (this.indexName.nonEmpty) {
        throw new UnsupportedOperationException("Index name is already set.")
      }

      if (indexName.isEmpty) {
        throw new IllegalArgumentException("Empty index name is not allowed.")
      }

      this.indexName = indexName
      this
    }

    /**
     * Updates column names for [[CoveringConfig]].
     *
     * Note: API signature supports passing one or more argument.
     *
     * @param indexedColumn  indexed column for the [[CoveringConfig]].
     * @param indexedColumns indexed columns for the [[CoveringConfig]].
     * @return an [[CoveringConfig.Builder]] object with updated indexed columns.
     */
    def indexBy(indexedColumn: String, indexedColumns: String*): Builder = {
      if (this.indexedColumns.nonEmpty) {
        throw new UnsupportedOperationException("Indexed columns are already set.")
      }

      this.indexedColumns = indexedColumn +: indexedColumns
      this
    }

    /**
     * Updates included columns for [[CoveringConfig]].
     *
     * Note: API signature supports passing one or more argument.
     *
     * @param includedColumn  included column for [[CoveringConfig]].
     * @param includedColumns included columns for [[CoveringConfig]].
     * @return an [[CoveringConfig.Builder]] object with updated included columns.
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
     * to [[CoveringConfig.Builder]].
     *
     * @return an [[CoveringConfig]] object.
     */
    def build(): IndexConfig = {
      IndexConfig(indexName, indexedColumns, includedColumns)
    }
  }

  /**
   *  Creates new [[CoveringConfig.Builder]] for constructing an [[CoveringConfig]].
   *
   * @return an [[CoveringConfig.Builder]] object.
   */
  def builder(): Builder = new Builder
}
