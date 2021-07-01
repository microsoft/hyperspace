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

package com.microsoft.hyperspace.index.types.dataskipping.sketch

import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.Column

/**
 * Represents a sketch specification for data skipping indexes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
trait Sketch {

  /**
   * Returns column names that this sketch can be applied.
   */
  def indexedColumns: Seq[String]

  /**
   * Returns column names that this sketch may reference in addition to the
   * indexed columns.
   */
  def auxiliaryColumns: Seq[String]

  /**
   * Returns column names that this sketch references.
   */
  final def referencedColumns: Seq[String] = indexedColumns ++ auxiliaryColumns

  /**
   * Returns a copy of this sketch with updated columns.
   *
   * @param columnMapping Mapping from old column names to new column names
   */
  def withNewColumns(columnMapping: Map[String, String]): Sketch

  /**
   * Returns aggregate functions that can be used to compute the actual sketch
   * values from source data.
   *
   * It should return a non-empty sequence and the length of the sequence
   * should be the same as [[numValues]].
   *
   * The column names should succintly describe the value.
   */
  def aggregateFunctions: Seq[Column]

  /**
   * Returns the number of the values this sketch creates per file.
   */
  def numValues: Int

  /**
   * Returns a human-readable string describing this sketch.
   */
  def toString: String

  /**
   * Returns true if and only if this sketch equals to that.
   *
   * Sketches should be considered equal when their types and columns are
   * equal.
   */
  def equals(that: Any): Boolean

  /**
   * Returns the hash code for this sketch.
   */
  def hashCode: Int
}
