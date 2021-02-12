package com.microsoft.hyperspace.index

sealed trait IndexConfigBase {
  val indexName: String

  def equals(obj: Any): Boolean

  def hashCode(): Int

  def toString: String
}

trait CoveringIndexConfigBase extends IndexConfigBase {
  /*
   * Columns from which index are created.
   */
  val indexedColumns: Seq[String]

  /*
   * Columns to be included with the indexed columns.
   */
  val includedColumns: Seq[String]
}

trait NonCoveringIndexConfigBase extends IndexConfigBase {}
