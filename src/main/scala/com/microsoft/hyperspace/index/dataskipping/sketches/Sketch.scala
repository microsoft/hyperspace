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

package com.microsoft.hyperspace.index.dataskipping.sketches

import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.types.DataType

import com.microsoft.hyperspace.index.dataskipping.expressions.ExpressionExtractor

/**
 * Represents a sketch specification for data skipping indexes.
 *
 * Sketch implementations should support serialization with Jackson.
 * Normally this should be done automatically without any special handling.
 * When serialized as JSON objects, a special field called "type" is used
 * to store the class name of the implementation. Therefore, the implementation
 * must not have a field named "type".
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
  def aggregateFunctions: Seq[AggregateFunction]

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

  /**
   * Converts the given predicate node for source data to an index predicate
   * that can be used to filter out unnecessary source files when applied to
   * index data.
   *
   * The returned predicate should evaluate to true for an index data row
   * if the corresponding source data file cannot be excluded. The returned
   * predicate should be used only to find files that cannot be skipped. In
   * other words, it must not be negated to find files that can be skipped,
   * because it can evaluate to null.
   *
   * The implementation should consider the given predicate as a single node,
   * not a tree that must be traversed recursively, because that part is
   * handled by the framework.
   *
   * @param predicate Source predicate node
   * @param resolvedExprs Sketch expressions that can be used to match
   *          expressions in the source predicate; for example,
   *          MinMaxSketch("A") will be given an expression corresponding to
   *          "A".
   * @param sketchValues Sketch value references in index data; for example,
   *          MinMaxSketch("A") will be given two expressions corresponding to
   *          Min(A) and Max(A) in the index data. If the predicate is
   *          convertible, the implementation should return a predicate
   *          composed of these sketch values.
   * @param nameMap Map used to normalize attributes in the source predicate by
   *          looking up the attribute name with ExprId; this is needed because
   *          the attribute name in the predicate may have different cases
   *          (lower/upper cases).
   * @return Converted predicate for index data
   */
  def convertPredicate(
      predicate: Expression,
      resolvedExprs: Seq[Expression],
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      valueExtractor: ExpressionExtractor): Option[Expression]
}
