package com.microsoft.hyperspace.index.configs.noncovering

/**
 * TODO Defines [[BloomFilterIndexConfig.Builder]] and relevant helper methods for enabling
 *  builder pattern for [[BloomFilterIndexConfig]].
 */
object BloomFilterIndexConfig {

  /**
   * Builder for [[BloomFilterIndexConfig]].
   */
  private[index] class Builder {

    private[this] var indexedColumn: String = ""
    private[this] var indexName: String = ""
  }

  /**
   * Creates new [[BloomFilterIndexConfig.Builder]] for constructing an [[BloomFilterIndexConfig]].
   *
   * @return an [[BloomFilterIndexConfig.Builder]] object.
   */
  def builder(): Builder = new Builder
}
