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

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

object HyperSpaceIndex {

  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
  )
  @JsonSubTypes(Array(
    new Type(value = classOf[CoveringIndex], name = "coveringIndex"),
    new Type(value = classOf[BloomFilterIndex], name = "bloomFilterIndex")
  ))
  trait IndexType {
    def kind: String

    def kindAbbr: String

    def properties: Properties.ExposeProperties
  }

  // IndexLogEntry-specific CoveringIndex that represents derived dataset.
  case class CoveringIndex(
                            coveringProperties: Properties.Covering
                          ) extends IndexType {

    override def kind: String = Kinds.covering

    override def kindAbbr: String = "CI"

    override def properties: Properties.ExposeProperties = coveringProperties
  }

  case class BloomFilterIndex(
                               bloomProperties: Properties.BloomFilter
                             ) extends IndexType {
    override def kind: String = Kinds.nonCovering

    override def kindAbbr: String = "BF"

    override def properties: Properties.ExposeProperties = bloomProperties
  }

  object Properties {
    trait ExposeProperties {
      def columns: CommonProperties.Columns

      def schemaString: String

      def properties: Map[String, String]
    }

    object CommonProperties {
      case class Columns(indexed: Seq[String], included: Seq[String])
    }

    case class Covering(columns: CommonProperties.Columns,
                        schemaString: String,
                        numBuckets: Int,
                        properties: Map[String, String]) extends ExposeProperties

    case class BloomFilter(columns: CommonProperties.Columns,
                           schemaString: String,
                           properties: Map[String, String]) extends ExposeProperties
  }
}
