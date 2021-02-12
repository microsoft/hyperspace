package com.microsoft.hyperspace.index

/**
 * All index supported in Hyperspace whose user facing config needs to be defined needs
 * to extend [[ConfigBase]] trait.
 */
sealed trait ConfigBase {
  val indexName: String

  def equals(obj: Any): Boolean

  def hashCode(): Int

  def toString: String
}

trait CoveringIndexConfigBase extends ConfigBase {
  /*
   * Columns from which index are created.
   */
  val indexedColumns: Seq[String]

  /*
   * Columns to be included with the indexed columns.
   */
  val includedColumns: Seq[String]
}

trait NonCoveringIndexConfigBase extends ConfigBase {}
