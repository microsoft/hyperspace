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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo, JsonValue}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

object HyperSpaceIndex {

  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "kind",
    defaultImpl = classOf[HashPartitionIndex]
  )
  @JsonSubTypes(Array(
    new Type(value = classOf[HashPartitionIndex], name = "hashPartitionIndex"),
    new Type(value = classOf[BloomFilterIndex], name = "bloomFilterIndex")
  ))
  trait IndexType {
    def kind: String

    def kindAbbr: String

    def properties: Properties.Properties
  }

  case class HashPartitionIndex(
                                 properties: Properties.HashPartition
                          ) extends IndexType {

    override def kind: String = "hashPartitionIndex"

    override def kindAbbr: String = "HP"
  }

  case class BloomFilterIndex(
                               properties: Properties.BloomFilter
                             ) extends IndexType {

    override def kind: String = "bloomFilterIndex"

    override def kindAbbr: String = "BF"
  }

  object Properties {
    @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.EXISTING_PROPERTY,
      property = "kind",
      defaultImpl = classOf[HashPartition]
    )
    @JsonSubTypes(Array(
      new Type(value = classOf[HashPartition], name = "hashPartitionIndex"),
      new Type(value = classOf[BloomFilter], name = "bloomFilterIndex")
    ))
    trait Properties {
      def columns: CommonProperties.Columns

      def schemaString: String

      def properties: Map[String, String]
    }

    object CommonProperties {
      case class Columns(indexed: Seq[String], included: Seq[String])
    }

    case class HashPartition(columns: CommonProperties.Columns,
                        schemaString: String,
                        numBuckets: Int,
                        properties: Map[String, String]) extends Properties

    case class BloomFilter(columns: CommonProperties.Columns,
                           schemaString: String,
                           properties: Map[String, String]) extends Properties
  }
}
