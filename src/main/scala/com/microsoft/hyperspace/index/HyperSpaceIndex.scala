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