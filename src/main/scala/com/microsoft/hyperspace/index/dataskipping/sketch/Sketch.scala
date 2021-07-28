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

package com.microsoft.hyperspace.index.dataskipping.sketch

import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

/**
 * Represents a sketch specification for data skipping indexes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
trait Sketch {

  /**
   * Returns the expressions this sketch is based on.
   */
  def expressions: Seq[(String, Option[DataType])]

  /**
   * Returns column names that this sketch can be applied.
   */
  def indexedColumns: Seq[String]

  /**
   * Returns column names that this sketch references.
   */
  def referencedColumns: Seq[String]

  /**
   * Returns a copy of this sketch with updated expressions.
   */
  def withNewExpressions(newExpressions: Seq[(String, Option[DataType])]): Sketch

  /**
   * Returns aggregate functions that can be used to compute the actual sketch
   * values from source data.
   */
  def aggregateFunctions: Seq[Expression]

  /**
   * Returns a human-readable string describing this sketch.
   */
  def toString: String

  /**
   * Returns true if and only if this sketch equals to that.
   *
   * Sketches should be considered equal when their types and expressions are
   * equal.
   */
  def equals(that: Any): Boolean

  /**
   * Returns the hash code for this sketch.
   */
  def hashCode: Int
}
