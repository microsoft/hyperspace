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

import com.microsoft.hyperspace.index.BloomFilterIndexConfig

/**
 * TODO Defines [[BloomFilterConfig.Builder]] and relevant helper methods for enabling
 *  builder pattern for [[BloomFilterConfig]].
 */
object BloomFilterConfig {

  /**
   * Builder for [[BloomFilterConfig]].
   */
  private[index] class Builder {

    private[this] var indexedColumn: String = ""
    private[this] var indexName: String = ""
    private[this] var fpp: Double = -1
    private[this] var expectedItems: Long = -1
    private[this] var numBits: Long = -1

    /**
     * Updates index name for [[CoveringConfig]].
     *
     * @param indexName index name for the [[BloomFilterConfig]].
     * @return an [[BloomFilterConfig.Builder]] object with updated index name.
     */
    def init(indexName: String, indexedColumn: String): Builder = {
      if (this.indexName.nonEmpty || this.indexedColumn.nonEmpty) {
        // TODO: Prevent creating index config if index already exists.
        throw new UnsupportedOperationException(
          "Bloom Filter index metadata already set can't override, " +
            "maybe try creating a new config.")
      }

      if (indexName.isEmpty || indexedColumn.isEmpty) {
        throw new IllegalArgumentException("Empty metadata names is not allowed.")
      }

      this.indexName = indexName
      this.indexedColumn = indexedColumn
      this
    }

    /**
     *
     * @param items
     * @return
     */
    def expectedNumItems(items: Long): Builder = {
      if (items < 1) {
        throw new IllegalArgumentException("Can't support the items value provided.")
      }

      this.expectedItems = items
      this
    }

    /**
     *
     * @param fpp
     * @return
     */
    def fppToSupport(fpp: Double): Builder = {
      if (fpp <= 0) {
        throw new IllegalArgumentException("Can't support the fpp value.")
      }

      this.fpp = fpp
      this
    }

    /**
     *
     * @param bits
     * @return
     */
    def numBitsToDefineBloomFilter(bits: Long): Builder = {
      if (bits < 1) {
        throw new IllegalArgumentException("Can't allow bits for storage be less than 1")
      }

      this.numBits = bits
      this
    }

    def build(): BloomFilterIndexConfig = {
      new BloomFilterIndexConfig(indexName, indexedColumn, expectedItems, fpp, numBits)
    }
  }

  /**
   * Creates new [[BloomFilterConfig.Builder]] for constructing an [[BloomFilterConfig]].
   *
   * @return an [[BloomFilterConfig.Builder]] object.
   */
  def builder(): Builder = new Builder
}
