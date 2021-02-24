package com.microsoft.hyperspace.index

/**
 * All index supported in Hyperspace whose user facing config needs to be defined needs
 * to extend [[HyperSpaceIndexConfig]] trait.
 */
sealed trait HyperSpaceIndexConfig {
  def indexName: String

  def kind: String
}

object Kinds extends Enumeration {
  type Kinds = String
  val clustered = "Clustered"
  val nonClustered: Kinds = "NonClustered"
  val covering: Kinds = nonClustered + "-CoveringIndex"
  val nonCovering: Kinds = nonClustered + "-NonCoveringIndex"

  def identifyKind(provided: Kinds): Kinds = {
    if (provided.contains(clustered)) {
      clustered
    } else {
      nonClustered
    }
  }
}

abstract class CoveringIndexConfig extends HyperSpaceIndexConfig {
  def kind: String = Kinds.covering

  /*
   * Columns from which index are created.
   */
  def indexedColumns: Seq[String]

  /*
   * Columns to be included with the indexed columns.
   */
  def includedColumns: Seq[String]
}

abstract class NonCoveringIndexConfig extends HyperSpaceIndexConfig {
  def kind: String = Kinds.nonCovering
}

case class IndexConfigBundle(config: HyperSpaceIndexConfig) {
}
