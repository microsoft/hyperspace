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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Max, Min}
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}
import org.apache.spark.sql.types.{ArrayType, DataType}

import com.microsoft.hyperspace.index.dataskipping.expressions._
import com.microsoft.hyperspace.index.dataskipping.util.ArrayUtils

/**
 * Sketch based on minimum and maximum values for a given expression.
 *
 * @param expr Expression from which min/max values are calculated
 * @param dataType Optional data type to specify the expected data type of the
 *         expression. If not specified, it is deduced automatically.
 *         If the actual data type of the expression is different from this,
 *         an error is thrown. Users are recommended to leave this parameter to
 *         None.
 */
case class MinMaxSketch(override val expr: String, override val dataType: Option[DataType] = None)
    extends SingleExprSketch[MinMaxSketch](expr, dataType) {
  override def name: String = "MinMax"

  override def withNewExpression(newExpr: (String, Option[DataType])): MinMaxSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[AggregateFunction] = {
    Min(parsedExpr) :: Max(parsedExpr) :: Nil
  }

  override def convertPredicate(
      predicate: Expression,
      resolvedExprs: Seq[Expression],
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      valueExtractor: ExpressionExtractor): Option[Expression] = {
    val min = sketchValues(0)
    val max = sketchValues(1)
    // TODO: Add third sketch value "hasNull" of type bool
    // true if the expr can be null in the file, false if otherwise
    // to optimize IsNull (can skip files with hasNull = false)
    // This can be also done as a separate sketch, e.g. HasNullSketch
    // Should evaluate which way is better
    val resolvedExpr = resolvedExprs.head
    val dataType = resolvedExpr.dataType
    val exprExtractor = NormalizedExprExtractor(resolvedExpr, nameMap)
    val ExprIsTrue = IsTrueExtractor(exprExtractor)
    val ExprIsFalse = IsFalseExtractor(exprExtractor)
    val ExprIsNotNull = IsNotNullExtractor(exprExtractor)
    val ExprEqualTo = EqualToExtractor(exprExtractor, valueExtractor)
    val ExprEqualNullSafe = EqualNullSafeExtractor(exprExtractor, valueExtractor)
    val ExprLessThan = LessThanExtractor(exprExtractor, valueExtractor)
    val ExprLessThanOrEqualTo = LessThanOrEqualExtractor(exprExtractor, valueExtractor)
    val ExprGreaterThan = LessThanExtractor(valueExtractor, exprExtractor)
    val ExprGreaterThanOrEqualTo = LessThanOrEqualExtractor(valueExtractor, exprExtractor)
    val ExprIn = InExtractor(exprExtractor, valueExtractor)
    val ExprInSet = InSetExtractor(exprExtractor)
    Option(predicate)
      .collect {
        case ExprIsTrue(_) => max
        case ExprIsFalse(_) => Not(min)
        case ExprIsNotNull(_) => IsNotNull(min)
        case ExprEqualTo(_, v) => And(LessThanOrEqual(min, v), GreaterThanOrEqual(max, v))
        case ExprEqualNullSafe(_, v) =>
          Or(IsNull(v), And(LessThanOrEqual(min, v), GreaterThanOrEqual(max, v)))
        case ExprLessThan(_, v) => LessThan(min, v)
        case ExprLessThanOrEqualTo(_, v) => LessThanOrEqual(min, v)
        case ExprGreaterThan(v, _) => GreaterThan(max, v)
        case ExprGreaterThanOrEqualTo(v, _) => GreaterThanOrEqual(max, v)
        case ExprIn(_, vs) =>
          vs.map(v => And(LessThanOrEqual(min, v), GreaterThanOrEqual(max, v))).reduceLeft(Or)
        case ExprInSet(_, vs) =>
          val sortedValues = Literal(
            ArrayData.toArrayData(
              ArrayUtils.toArray(
                vs.filter(_ != null).toArray.sorted(TypeUtils.getInterpretedOrdering(dataType)),
                dataType)),
            ArrayType(dataType, containsNull = false))
          LessThanOrEqual(ElementAt(sortedValues, SortedArrayLowerBound(sortedValues, min)), max)
        // TODO: StartsWith, Like with constant prefix
      }
  }
}
