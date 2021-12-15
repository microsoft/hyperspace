/*
 * Copyright (2021) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.index.covering

import java.util.Locale

import com.microsoft.hyperspace.index._

trait CoveringIndexConfigTrait extends IndexConfigTrait {
  def indexName: String
  def indexedColumns: Seq[String]
  def includedColumns: Seq[String]

  if (indexName.isEmpty || indexedColumns.isEmpty) {
    throw new IllegalArgumentException("Empty index name or indexed columns are not allowed.")
  }

  protected def toLowerCase(seq: Seq[String]): Seq[String] = seq.map(_.toLowerCase(Locale.ROOT))

  val lowerCaseIndexedColumns = toLowerCase(indexedColumns)
  val lowerCaseIncludedColumns = toLowerCase(includedColumns)
  val lowerCaseIncludedColumnsSet = lowerCaseIncludedColumns.toSet

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
      case CoveringIndexConfig(thatIndexName, thatIndexedColumns, thatIncludedColumns) =>
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

  override def referencedColumns: Seq[String] = indexedColumns ++ includedColumns
}
