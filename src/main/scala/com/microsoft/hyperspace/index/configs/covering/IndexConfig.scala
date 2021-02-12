package com.microsoft.hyperspace.index.configs.covering

import com.microsoft.hyperspace.index.IndexConfig

/**
 * Defines [[IndexConfig.Builder]] and relevant helper methods for enabling builder pattern for
 * [[IndexConfig]].
 */
object IndexConfig {

  /**
   * Builder for [[IndexConfig]].
   */
  private[index] class Builder {

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
    def build(): IndexConfig = {
      new IndexConfig(indexName, indexedColumns, includedColumns)
    }
  }

  /**
   *  Creates new [[IndexConfig.Builder]] for constructing an [[IndexConfig]].
   *
   * @return an [[IndexConfig.Builder]] object.
   */
  def builder(): Builder = new Builder
}
