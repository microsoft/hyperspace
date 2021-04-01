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

import java.util.Locale

import com.microsoft.hyperspace.index.configs.CoveringConfig

/**
 * IndexConfig specifies the configuration of an index.
 * Associated Builder [[CoveringConfig.builder()]]
 *
 * @param indexName Index name.
 * @param indexedColumns Columns from which an index is created.
 * @param includedColumns Columns to be included in the index.
 */
case class IndexConfig(
    indexName: String,
    indexedColumns: Seq[String],
    includedColumns: Seq[String] = Seq())
    extends CoveringIndexConfig {
  if (indexName.isEmpty || indexedColumns.isEmpty) {
    throw new IllegalArgumentException("Empty index name or indexed columns are not allowed.")
  }

  val lowerCaseIndexedColumns: Seq[String] = toLowerCase(indexedColumns)
  val lowerCaseIncludedColumns: Seq[String] = toLowerCase(includedColumns)
  val lowerCaseIncludedColumnsSet: Set[String] = lowerCaseIncludedColumns.toSet

  if (lowerCaseIndexedColumns.toSet.size < lowerCaseIndexedColumns.size) {
    throw new IllegalArgumentException("Duplicate indexed column names are not allowed.")
  }

  if (lowerCaseIncludedColumnsSet.size < lowerCaseIncludedColumns.size) {
    throw new IllegalArgumentException("Duplicate included column names are not allowed.")
  }

  for (indexedColumn <- lowerCaseIndexedColumns) {
    if (lowerCaseIncludedColumns.contains(indexedColumn)) {
      throw new IllegalArgumentException(
        "Duplicate column names in indexed/included columns are not allowed.")
    }
  }

  override def equals(that: Any): Boolean = {
    that match {
      case IndexConfig(thatIndexName, thatIndexedColumns, thatIncludedColumns) =>
        indexName.equalsIgnoreCase(thatIndexName) &&
          lowerCaseIndexedColumns.equals(toLowerCase(thatIndexedColumns)) &&
          lowerCaseIncludedColumnsSet.equals(toLowerCase(thatIncludedColumns).toSet)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    lowerCaseIndexedColumns.hashCode + lowerCaseIncludedColumnsSet.hashCode
  }

  override def toString: String = {
    val indexedColumnNames = lowerCaseIndexedColumns.mkString(", ")
    val includedColumnNames = lowerCaseIncludedColumns.mkString(", ")
    s"[indexName: $indexName; indexedColumns: $indexedColumnNames; " +
      s"includedColumns: $includedColumnNames]"
  }

  private def toLowerCase(seq: Seq[String]): Seq[String] = seq.map(_.toLowerCase(Locale.ROOT))
}

case class BloomFilterIndexConfig(
    indexName: String,
    indexedColumn: String,
    expectedNumItems: Long,
    fpp: Double = 0.03, // DEFAULT FPP in Apache Spark Bloom Filter Implementation
    numBits: Long = -1)
    extends NonCoveringIndexConfig {
  if (indexName.isEmpty || indexedColumn.isEmpty || expectedNumItems < 1) {
    throw new IllegalArgumentException(
      "Empty index name or indexed column or expected items less than 1 are not allowed.")
  }

  if (fpp != -1 && fpp < 0) {
    throw new IllegalArgumentException("False positive probability cannot be negative.")
  }

  if (numBits != -1 && numBits < 0) {
    throw new IllegalArgumentException("Bits given for ")
  }

  val lowerCaseIndexedColumn: String = indexedColumn.toLowerCase(Locale.ROOT)

  override def equals(that: Any): Boolean = {
    that match {
      case BloomFilterIndexConfig(
          thatIndexName,
          thatIndexedColumn,
          thatExpectedItems,
          thatFpp,
          thatBits) =>
        indexName.equalsIgnoreCase(thatIndexName) &&
          lowerCaseIndexedColumn.equals(thatIndexedColumn.toLowerCase(Locale.ROOT)) &&
          expectedNumItems == thatExpectedItems &&
          ((fpp == -1 && thatFpp == -1) || fpp == thatFpp) &&
          ((numBits == -1 && thatBits == -1) || numBits == thatBits)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    (indexName.hashCode * (indexedColumn.hashCode + expectedNumItems
      .hashCode())) % scala.Int.MaxValue
  }

  override def toString: String = {
    s"[indexName: $indexName; indexedColumn: $indexedColumn; " +
      s"ExpectedItems: $expectedNumItems; FPP: $fpp; NumBitsUsed: $numBits;]"
  }
}

object Configs {

  // TODO - prints info table about all types of index supported by hyperspace
  def printAllIndexConfigInfo(): String = {
    s""
  }
}
