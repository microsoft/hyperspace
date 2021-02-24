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
